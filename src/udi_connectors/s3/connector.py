import asyncio
import logging
from collections.abc import AsyncIterator
from io import BytesIO
from pathlib import PurePosixPath
from typing import Any, Literal

import boto3
import pandas as pd
import pyarrow as pa
from botocore.exceptions import ClientError
from udi_connectors._registry import Source, Target
from udi_packages import (
    Batch,
    BatchMetadata,
    ConnectorError,
    ExtractResult,
    LoadResult,
    TargetCapabilities,
)

from .config import S3Config

logger = logging.getLogger(__name__)


@Source("s3")
@Target("s3")
class S3Connector:
    """S3-backed data lake connector.

    Serves as both a Target (landing raw/curated batches) and a Source (a
    dependency-free fallback reader over a landed prefix, for use when
    Athena/Glue isn't provisioned). Keys are laid out as:

        {prefix}/{zone}/{connection_id}/{table_name}/dt={extracted_at:%Y-%m-%d}/{batch_id}.ext
    """

    Config = S3Config

    def __init__(self):
        self._client = None
        self._config: S3Config | None = None

    async def connect(self, config: S3Config) -> None:
        logger.info("Connecting S3: bucket=%s region=%s prefix=%s zone=%s", config.bucket_name, config.region, config.prefix, config.zone)
        try:
            self._config = config
            kwargs: dict[str, Any] = {
                "region_name": config.region,
            }
            if config.endpoint_url:
                kwargs["endpoint_url"] = config.endpoint_url
            if config.access_key and config.secret_key:
                kwargs["aws_access_key_id"] = config.access_key
                kwargs["aws_secret_access_key"] = config.secret_key
            if config.session_token:
                kwargs["aws_session_token"] = config.session_token

            loop = asyncio.get_running_loop()
            self._client = await loop.run_in_executor(
                None, lambda: boto3.client("s3", **kwargs)
            )

            await loop.run_in_executor(
                None, lambda: self._client.head_bucket(Bucket=config.bucket_name)
            )
            logger.info("Connected S3: %s", config.bucket_name)
        except ClientError as e:
            raise ConnectorError(
                f"Failed to connect to S3 bucket '{config.bucket_name}': {e}",
                "s3",
                retryable=True,
            ) from e
        except Exception as e:
            raise ConnectorError(
                f"S3 connection error: {e}", "s3", retryable=False
            ) from e

    async def disconnect(self) -> None:
        logger.info("Disconnecting S3")
        if self._client:
            await asyncio.get_running_loop().run_in_executor(None, self._client.close)
            self._client = None

    async def test_connection(self) -> bool:
        if not self._client or not self._config:
            return False
        try:
            await asyncio.get_running_loop().run_in_executor(
                None, lambda: self._client.head_bucket(Bucket=self._config.bucket_name)
            )
            return True
        except Exception:
            return False

    # ------------------------------------------------------------------
    # Key / prefix layout
    # ------------------------------------------------------------------

    def _table_prefix(self, table_name: str, config: S3Config, zone: str | None = None) -> str:
        if not config.connection_id:
            # No connection_id => the original flat layout, byte-for-byte.
            # This keeps every existing caller that doesn't opt into the
            # zone/connection_id convention (Stage 1's current call path
            # included) landing exactly where it always has.
            return f"{table_name}/"
        zone = zone or config.zone
        parts = [config.prefix, zone, config.connection_id, table_name]
        return "/".join(p.strip("/") for p in parts if p) + "/"

    def _build_key(self, table_name: str, batch: Batch, config: S3Config) -> str:
        if not config.connection_id:
            key = str(PurePosixPath(table_name) / batch.metadata.batch_id)
        else:
            zone = batch.metadata.zone or config.zone
            dt = batch.metadata.extracted_at.strftime("%Y-%m-%d")
            parts = [config.prefix, zone, config.connection_id, table_name, f"dt={dt}", batch.metadata.batch_id]
            key = "/".join(p.strip("/") for p in parts if p)

        if config.file_format == "parquet":
            key += ".parquet"
        elif config.file_format == "csv":
            key += ".csv"
        elif config.file_format == "jsonl":
            key += ".jsonl"
        if config.compression != "none" and config.file_format != "parquet":
            ext = ".gz" if config.compression == "gzip" else ".snappy"
            key += ext
        return key

    # ------------------------------------------------------------------
    # Target: load (append / upsert)
    # ------------------------------------------------------------------

    async def load(
        self,
        batches: AsyncIterator[Batch],
        table_name: str,
        mode: Literal["append", "upsert"] = "append",
        merge_keys: list[str] | None = None,
    ) -> LoadResult:
        if not self._client or not self._config:
            raise ConnectorError("S3Connector is not connected", "s3", retryable=False)

        config = self._config

        if mode == "upsert":
            return await self._load_upsert(batches, table_name, config, merge_keys or [])

        logger.info("Loading to S3 (%s): table=%s", mode, table_name)
        semaphore = asyncio.Semaphore(config.max_concurrent_uploads)
        tasks: list[asyncio.Task] = []
        total_rows = 0
        batch_count = 0

        async for batch in batches:
            batch_count += 1
            total_rows += batch.metadata.row_count
            task = asyncio.create_task(
                self._upload_batch_with_semaphore(semaphore, batch, table_name, config)
            )
            tasks.append(task)

        errors: list[str] = []
        if tasks:
            results = await asyncio.gather(*tasks, return_exceptions=True)
            for r in results:
                if isinstance(r, Exception):
                    errors.append(str(r))

        logger.info(
            "S3 load complete: table=%s rows=%d files=%d errors=%d",
            table_name, total_rows, batch_count, len(errors),
        )
        return LoadResult(
            destination_type="s3",
            table_name=table_name,
            rows_loaded=total_rows,
            batch_count=batch_count,
            errors=errors,
        )

    async def _load_upsert(
        self,
        batches: AsyncIterator[Batch],
        table_name: str,
        config: S3Config,
        merge_keys: list[str],
    ) -> LoadResult:
        if not merge_keys:
            raise ConnectorError("upsert mode requires merge_keys", "s3", retryable=False)

        incoming_tables: list[pa.Table] = []
        async for batch in batches:
            incoming_tables.append(batch.data)
        if not incoming_tables:
            return LoadResult(destination_type="s3", table_name=table_name, rows_loaded=0, batch_count=0, errors=[])

        incoming = pa.concat_tables(incoming_tables, promote_options="default")

        existing_keys = await self._list_object_keys(table_name, config)
        existing_table = await self._read_objects(existing_keys, config) if existing_keys else None

        merged = await asyncio.get_running_loop().run_in_executor(
            None, self._merge_upsert, existing_table, incoming, merge_keys
        )

        # Delete-then-rewrite: object stores have no native MERGE, so the
        # existing partition is compacted into fresh files. Fine at moderate
        # table sizes; a real table format (Iceberg/Delta/Hudi) is the right
        # answer once this needs to scale.
        if existing_keys:
            await self._delete_objects(existing_keys, config)

        errors: list[str] = []
        written = 0
        batch_size = config.batch_size
        chunk_num = 0
        for offset in range(0, merged.num_rows, batch_size):
            chunk = merged.slice(offset, batch_size)
            chunk_num += 1
            synth_batch = Batch(
                data=chunk,
                metadata=BatchMetadata(
                    source_name="s3",
                    table_name=table_name,
                    batch_id=f"{table_name}-upsert-{chunk_num}",
                    row_count=chunk.num_rows,
                    byte_size=chunk.nbytes,
                    schema=chunk.schema,
                    zone=config.zone,
                ),
            )
            try:
                await self._upload_batch(synth_batch, table_name, config)
                written += chunk.num_rows
            except Exception as e:
                errors.append(str(e))

        logger.info(
            "S3 upsert complete: table=%s existing=%d incoming=%d merged=%d files=%d",
            table_name,
            existing_table.num_rows if existing_table is not None else 0,
            incoming.num_rows,
            merged.num_rows,
            chunk_num,
        )
        return LoadResult(
            destination_type="s3",
            table_name=table_name,
            rows_loaded=written,
            batch_count=chunk_num,
            errors=errors,
        )

    def _merge_upsert(self, existing: pa.Table | None, incoming: pa.Table, keys: list[str]) -> pa.Table:
        if existing is None or existing.num_rows == 0:
            return incoming
        existing_df = existing.to_pandas()
        incoming_df = incoming.to_pandas()
        incoming_index = incoming_df.set_index(keys).index
        remaining = existing_df[~existing_df.set_index(keys).index.isin(incoming_index)]
        merged_df = pd.concat([remaining, incoming_df], ignore_index=True, sort=False)
        return pa.Table.from_pandas(merged_df, preserve_index=False)

    async def _upload_batch_with_semaphore(
        self,
        semaphore: asyncio.Semaphore,
        batch: Batch,
        table_name: str,
        config: S3Config,
    ) -> None:
        async with semaphore:
            await self._upload_batch(batch, table_name, config)

    async def _upload_batch(
        self, batch: Batch, table_name: str, config: S3Config
    ) -> None:
        data = await self._serialize_batch(batch, config.file_format, config.compression)
        key = self._build_key(table_name, batch, config)

        logger.info("Uploading to s3://%s/%s (%d rows)", config.bucket_name, key, batch.metadata.row_count)
        for attempt in range(config.retry_count):
            try:
                await asyncio.get_running_loop().run_in_executor(
                    None,
                    lambda: self._client.put_object(Bucket=config.bucket_name, Key=key, Body=data),
                )
                logger.debug("Uploaded s3://%s/%s", config.bucket_name, key)
                return
            except ClientError:
                if attempt == config.retry_count - 1:
                    raise
                await asyncio.sleep(config.retry_delay * (2 ** attempt))

        raise ConnectorError(
            f"Failed to upload {key} after {config.retry_count} attempts",
            "s3",
            retryable=True,
        )

    async def _serialize_batch(
        self, batch: Batch, file_format: str, compression: str
    ) -> BytesIO:
        buf = BytesIO()
        table = batch.data

        if file_format == "parquet":
            comp = "snappy" if compression == "snappy" else compression
            import pyarrow.parquet as pq
            pq.write_table(table, buf, compression=comp)
        elif file_format == "csv":
            import pyarrow.csv as csv
            write_options = csv.WriteOptions()
            csv.write_csv(table, buf, write_options)
        elif file_format == "jsonl":
            import pyarrow.json as pj
            pj.write_json(table, buf)
        else:
            raise ConnectorError(
                f"Unsupported file format: {file_format}", "s3", retryable=False
            )

        buf.seek(0)
        return buf

    def get_capabilities(self) -> TargetCapabilities:
        return TargetCapabilities(
            supports_batch_write=True,
            supports_parallel_uploads=True,
            max_batch_size=100_000,
        )

    # ------------------------------------------------------------------
    # Target: dataset_exists / get_schema
    # ------------------------------------------------------------------

    async def clear(self, table_name: str) -> int:
        """Delete every object under this table's zone/connection prefix.

        Used by Stage 3 to sweep Stage 2's `_staging` increment once it has
        been folded into the published dataset. Returns the number deleted.
        """
        if not self._client or not self._config:
            raise ConnectorError("S3Connector is not connected", "s3", retryable=False)
        config = self._config
        keys = await self._list_object_keys(table_name, config)
        if keys:
            await self._delete_objects(keys, config)
        return len(keys)

    async def dataset_exists(self, table_name: str) -> bool:
        if not self._client or not self._config:
            raise ConnectorError("S3Connector is not connected", "s3", retryable=False)
        config = self._config
        prefix = self._table_prefix(table_name, config)
        loop = asyncio.get_running_loop()
        resp = await loop.run_in_executor(
            None,
            lambda: self._client.list_objects_v2(Bucket=config.bucket_name, Prefix=prefix, MaxKeys=1),
        )
        return resp.get("KeyCount", 0) > 0

    async def get_schema(self, table_name: str) -> pa.Schema | None:
        if not self._client or not self._config:
            raise ConnectorError("S3Connector is not connected", "s3", retryable=False)
        config = self._config
        keys = await self._list_object_keys(table_name, config, limit=1)
        if not keys:
            return None
        table = await self._read_objects(keys, config)
        return table.schema if table is not None else None

    # ------------------------------------------------------------------
    # Source: extract / list / checkpoint
    # ------------------------------------------------------------------

    async def list_tables(self, config: S3Config) -> list[str]:
        connection_id = config.connection_id or "unassigned"
        parts = [config.prefix, config.zone, connection_id]
        prefix = "/".join(p.strip("/") for p in parts if p) + "/"
        loop = asyncio.get_running_loop()

        def _list():
            names = []
            paginator = self._client.get_paginator("list_objects_v2")
            for page in paginator.paginate(Bucket=config.bucket_name, Prefix=prefix, Delimiter="/"):
                for cp in page.get("CommonPrefixes", []):
                    name = cp["Prefix"][len(prefix):].strip("/")
                    # "{table}__staging" is the pipeline's Stage 2 staging
                    # convention (see udi_connectors.pipeline) — not a
                    # published dataset in its own right.
                    if name and not name.endswith("__staging"):
                        names.append(name)
            return sorted(names)

        return await loop.run_in_executor(None, _list)

    async def list_databases(self, config: S3Config) -> list[str]:
        return [config.bucket_name]

    async def get_checkpoint(self, table_name: str) -> dict | None:
        # The direct S3 reader has no incremental cursor of its own — Stage 2
        # orchestration checkpoints by resized batch after rechunking instead.
        return None

    def supports_incremental(self) -> bool:
        return False

    async def extract(
        self,
        table_name: str,
        config: S3Config,
        columns: list[str] | None = None,
        filter_predicate: str | None = None,
    ) -> ExtractResult:
        cfg = config or self._config
        if not cfg:
            raise ConnectorError("S3Connector is not connected", "s3", retryable=False)

        keys = await self._list_object_keys(table_name, cfg)
        if filter_predicate:
            keys = [k for k in keys if filter_predicate in k]
        logger.info("Extracting from S3: table=%s zone=%s objects=%d", table_name, cfg.zone, len(keys))

        async def batch_generator() -> AsyncIterator[Batch]:
            for key in keys:
                table = await self._read_object(key, cfg)
                if columns:
                    table = table.select(columns)
                yield Batch(
                    data=table,
                    metadata=BatchMetadata(
                        source_name="s3",
                        table_name=table_name,
                        batch_id=PurePosixPath(key).stem,
                        row_count=table.num_rows,
                        byte_size=table.nbytes,
                        schema=table.schema,
                        zone=cfg.zone,
                    ),
                )

        return ExtractResult(batches=batch_generator())

    # ------------------------------------------------------------------
    # Internal read/list/delete helpers
    # ------------------------------------------------------------------

    async def _list_object_keys(self, table_name: str, config: S3Config, limit: int | None = None) -> list[str]:
        prefix = self._table_prefix(table_name, config)
        loop = asyncio.get_running_loop()

        def _list():
            keys: list[str] = []
            paginator = self._client.get_paginator("list_objects_v2")
            for page in paginator.paginate(Bucket=config.bucket_name, Prefix=prefix):
                for obj in page.get("Contents", []):
                    keys.append(obj["Key"])
                    if limit and len(keys) >= limit:
                        return keys
            return keys

        return await loop.run_in_executor(None, _list)

    async def _read_object(self, key: str, config: S3Config) -> pa.Table:
        loop = asyncio.get_running_loop()

        def _get() -> bytes:
            resp = self._client.get_object(Bucket=config.bucket_name, Key=key)
            return resp["Body"].read()

        raw = await loop.run_in_executor(None, _get)
        return self._deserialize(raw, config.file_format)

    async def _read_objects(self, keys: list[str], config: S3Config) -> pa.Table | None:
        if not keys:
            return None
        tables = [await self._read_object(key, config) for key in keys]
        return pa.concat_tables(tables, promote_options="default") if tables else None

    def _deserialize(self, raw: bytes, file_format: str) -> pa.Table:
        buf = BytesIO(raw)
        if file_format == "parquet":
            import pyarrow.parquet as pq
            return pq.read_table(buf)
        elif file_format == "csv":
            import pyarrow.csv as csv
            return csv.read_csv(buf)
        elif file_format == "jsonl":
            import pyarrow.json as pj
            return pj.read_json(buf)
        raise ConnectorError(f"Unsupported file format: {file_format}", "s3", retryable=False)

    async def _delete_objects(self, keys: list[str], config: S3Config) -> None:
        loop = asyncio.get_running_loop()

        def _delete():
            for i in range(0, len(keys), 1000):
                chunk = keys[i:i + 1000]
                self._client.delete_objects(
                    Bucket=config.bucket_name,
                    Delete={"Objects": [{"Key": k} for k in chunk]},
                )

        await loop.run_in_executor(None, _delete)
