import asyncio
import logging
from collections.abc import AsyncIterator
from io import BytesIO
from pathlib import PurePosixPath
from typing import Any

import boto3
import pyarrow as pa
from botocore.exceptions import ClientError
from udi_connectors._registry import Target
from udi_packages import (
    Batch,
    BatchMetadata,
    ConnectorError,
    LoadResult,
    TargetCapabilities,
)

from .config import S3Config

logger = logging.getLogger(__name__)


@Target("s3")
class S3Connector:
    Config = S3Config

    def __init__(self):
        self._client = None
        self._config: S3Config | None = None

    async def connect(self, config: S3Config) -> None:
        logger.info("Connecting S3: bucket=%s region=%s prefix=%s", config.bucket_name, config.region, config.prefix)
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

    async def load(self, batches: AsyncIterator[Batch], table_name: str) -> LoadResult:
        if not self._client or not self._config:
            raise ConnectorError("S3Connector is not connected", "s3", retryable=False)

        logger.info("Loading to S3: table=%s", table_name)
        config = self._config
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

        key = str(PurePosixPath(table_name) / batch.metadata.batch_id)
        if config.file_format == "parquet":
            key += ".parquet"
        elif config.file_format == "csv":
            key += ".csv"
        elif config.file_format == "jsonl":
            key += ".jsonl"
        if config.compression != "none" and config.file_format != "parquet":
            ext = ".gz" if config.compression == "gzip" else ".snappy"
            key += ext

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
