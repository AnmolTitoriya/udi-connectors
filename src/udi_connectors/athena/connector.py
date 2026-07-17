import asyncio
import logging
import time
from collections.abc import AsyncIterator
from typing import Any

import boto3
import pandas as pd
import pyarrow as pa
from botocore.exceptions import ClientError
from udi_connectors._registry import Source
from udi_packages import (
    Batch,
    BatchMetadata,
    CheckpointFile,
    ConnectorError,
    ExtractResult,
    ConnectionError as ConnConnectionError,
)

from .config import AthenaConfig

logger = logging.getLogger(__name__)

_FAILED_STATES = {"FAILED", "CANCELLED"}

_HIVE_TYPE_MAP: dict[str, pa.DataType] = {
    "boolean": pa.bool_(),
    "tinyint": pa.int8(),
    "smallint": pa.int16(),
    "int": pa.int32(),
    "integer": pa.int32(),
    "bigint": pa.int64(),
    "float": pa.float32(),
    "real": pa.float32(),
    "double": pa.float64(),
    "decimal": pa.decimal128(38, 10),
    "string": pa.string(),
    "varchar": pa.string(),
    "char": pa.string(),
    "date": pa.date32(),
    "timestamp": pa.timestamp("us"),
    "binary": pa.binary(),
    "varbinary": pa.binary(),
}


@Source("athena")
class AthenaConnector:
    Config = AthenaConfig

    def __init__(self):
        self._client = None
        self._config: AthenaConfig | None = None

    async def connect(self, config: AthenaConfig) -> None:
        logger.info(
            "Connecting Athena: region=%s catalog=%s database=%s workgroup=%s",
            config.region, config.catalog, config.database, config.workgroup,
        )
        try:
            self._config = config
            kwargs: dict[str, Any] = {"region_name": config.region}
            if config.access_key and config.secret_key:
                kwargs["aws_access_key_id"] = config.access_key
                kwargs["aws_secret_access_key"] = config.secret_key
            if config.session_token:
                kwargs["aws_session_token"] = config.session_token

            loop = asyncio.get_running_loop()
            self._client = await loop.run_in_executor(None, lambda: boto3.client("athena", **kwargs))
            await loop.run_in_executor(
                None, lambda: self._client.get_work_group(WorkGroup=config.workgroup)
            )
            logger.info("Connected Athena: workgroup=%s", config.workgroup)
        except ClientError as e:
            raise ConnConnectionError(
                f"Failed to connect to Athena workgroup '{config.workgroup}': {e}", "athena"
            ) from e
        except Exception as e:
            raise ConnConnectionError(f"Athena connection error: {e}", "athena") from e

    async def disconnect(self) -> None:
        logger.info("Disconnecting Athena")
        if self._client:
            await asyncio.get_running_loop().run_in_executor(None, self._client.close)
            self._client = None

    async def test_connection(self) -> bool:
        if not self._client or not self._config:
            return False
        try:
            await asyncio.get_running_loop().run_in_executor(
                None, lambda: self._client.get_work_group(WorkGroup=self._config.workgroup)
            )
            return True
        except Exception:
            return False

    async def list_databases(self, config: AthenaConfig) -> list[str]:
        def _list():
            names = []
            paginator = self._client.get_paginator("list_databases")
            for page in paginator.paginate(CatalogName=config.catalog):
                names.extend(db["Name"] for db in page["DatabaseList"])
            return sorted(names)

        return await asyncio.get_running_loop().run_in_executor(None, _list)

    async def list_tables(self, config: AthenaConfig) -> list[str]:
        def _list():
            names = []
            paginator = self._client.get_paginator("list_table_metadata")
            for page in paginator.paginate(CatalogName=config.catalog, DatabaseName=config.database):
                names.extend(t["Name"] for t in page["TableMetadataList"])
            return sorted(names)

        return await asyncio.get_running_loop().run_in_executor(None, _list)

    async def get_schema(self, table_name: str) -> pa.Schema:
        config = self._config

        def _fetch():
            resp = self._client.get_table_metadata(
                CatalogName=config.catalog, DatabaseName=config.database, TableName=table_name,
            )
            return resp["TableMetadata"]["Columns"]

        columns = await asyncio.get_running_loop().run_in_executor(None, _fetch)
        return self._columns_to_arrow_schema(columns)

    def _columns_to_arrow_schema(self, columns: list[dict]) -> pa.Schema:
        fields = [
            pa.field(col["Name"], self._hive_type_to_arrow(col["Type"]), nullable=True)
            for col in columns
        ]
        return pa.schema(fields)

    def _hive_type_to_arrow(self, hive_type: str) -> pa.DataType:
        base = hive_type.lower().split("(")[0].split("<")[0].strip()
        return _HIVE_TYPE_MAP.get(base, pa.string())

    def _build_query(
        self,
        table_name: str,
        config: AthenaConfig,
        columns: list[str] | None,
        filter_predicate: str | None,
    ) -> str:
        col_list = ", ".join(columns) if columns else "*"

        where_parts = []
        if filter_predicate:
            where_parts.append(filter_predicate)

        incremental_col = config.incremental_column
        order_clause = ""
        if incremental_col:
            order_clause = f"ORDER BY {incremental_col}"
            if config.checkpoint_file:
                cp = CheckpointFile(config.checkpoint_file)
                checkpoint = cp.get(table_name)
                if checkpoint is not None:
                    quoted = f"'{checkpoint}'" if isinstance(checkpoint, str) else str(checkpoint)
                    where_parts.append(f"{incremental_col} > {quoted}")

        where_clause = f"WHERE {' AND '.join(where_parts)}" if where_parts else ""
        qualified_table = f'"{config.database}"."{table_name}"'
        return f"SELECT {col_list} FROM {qualified_table} {where_clause} {order_clause}".strip()

    def _start_and_wait(self, query: str, config: AthenaConfig) -> str:
        exec_kwargs: dict[str, Any] = {
            "QueryString": query,
            "QueryExecutionContext": {"Database": config.database, "Catalog": config.catalog},
            "WorkGroup": config.workgroup,
        }
        if config.output_location:
            exec_kwargs["ResultConfiguration"] = {"OutputLocation": config.output_location}

        resp = self._client.start_query_execution(**exec_kwargs)
        query_execution_id = resp["QueryExecutionId"]

        for _ in range(config.max_poll_attempts):
            status = self._client.get_query_execution(QueryExecutionId=query_execution_id)
            state = status["QueryExecution"]["Status"]["State"]
            if state == "SUCCEEDED":
                return query_execution_id
            if state in _FAILED_STATES:
                reason = status["QueryExecution"]["Status"].get("StateChangeReason", "unknown error")
                raise ConnectorError(f"Athena query failed ({state}): {reason}", "athena", retryable=True)
            time.sleep(config.poll_interval)

        raise ConnectorError(
            f"Athena query timed out after {config.max_poll_attempts} polls", "athena", retryable=True
        )

    def _fetch_page(self, query_execution_id: str, next_token: str | None):
        kwargs: dict[str, Any] = {"QueryExecutionId": query_execution_id, "MaxResults": 1000}
        if next_token:
            kwargs["NextToken"] = next_token
        resp = self._client.get_query_results(**kwargs)
        result_set = resp["ResultSet"]
        column_info = result_set["ResultSetMetadata"]["ColumnInfo"]
        return result_set["Rows"], resp.get("NextToken"), column_info

    def _convert_value(self, raw: str | None, athena_type: str) -> Any:
        if raw is None:
            return None
        t = athena_type.lower()
        try:
            if t == "boolean":
                return raw.lower() == "true"
            if t in ("tinyint", "smallint", "integer", "bigint"):
                return int(raw)
            if t in ("float", "real", "double", "decimal"):
                return float(raw)
            return raw
        except ValueError:
            return raw

    def _row_to_values(self, row: dict, column_info: list[dict]) -> list[Any]:
        data = row["Data"]
        return [
            self._convert_value(data[i].get("VarCharValue") if i < len(data) else None, col["Type"])
            for i, col in enumerate(column_info)
        ]

    def _rows_to_batch(
        self, rows: list[dict], column_info: list[dict], table_name: str, batch_num: int
    ) -> Batch:
        col_names = [c["Name"] for c in column_info]
        records = [self._row_to_values(row, column_info) for row in rows]
        df = pd.DataFrame(records, columns=col_names)
        table = pa.Table.from_pandas(df)
        logger.info("Batch %d: %d rows, %d bytes", batch_num, len(df), table.nbytes)
        return Batch(
            data=table,
            metadata=BatchMetadata(
                source_name="athena",
                table_name=table_name,
                batch_id=f"{table_name}_{batch_num}",
                row_count=len(df),
                byte_size=table.nbytes,
                schema=table.schema,
            ),
        )

    async def extract(
        self,
        table_name: str,
        config: AthenaConfig,
        columns: list[str] | None = None,
        filter_predicate: str | None = None,
    ) -> ExtractResult:
        logger.info("Extracting Athena table: %s", table_name)
        loop = asyncio.get_running_loop()
        try:
            query = self._build_query(table_name, config, columns, filter_predicate)
            logger.debug("Athena query: %s", query)
            query_execution_id = await loop.run_in_executor(None, self._start_and_wait, query, config)
            logger.info("Athena query succeeded: query_execution_id=%s", query_execution_id)
        except ConnectorError:
            raise
        except Exception as e:
            raise ConnectorError(
                f"Athena extract failed for {table_name}: {e}", "athena", retryable=True
            ) from e

        async def batch_generator() -> AsyncIterator[Batch]:
            batch_num = 0
            next_token: str | None = None
            pending_rows: list[dict] = []
            column_info: list[dict] | None = None
            first_page = True

            while True:
                rows, next_token, column_info = await loop.run_in_executor(
                    None, self._fetch_page, query_execution_id, next_token
                )
                if first_page:
                    rows = rows[1:] if rows else rows  # Athena repeats the header as row 0
                    first_page = False
                pending_rows.extend(rows)

                while len(pending_rows) >= config.batch_size:
                    chunk, pending_rows = pending_rows[: config.batch_size], pending_rows[config.batch_size :]
                    yield self._rows_to_batch(chunk, column_info, table_name, batch_num)
                    batch_num += 1

                if not next_token:
                    break

            if pending_rows:
                yield self._rows_to_batch(pending_rows, column_info, table_name, batch_num)

        return ExtractResult(batches=batch_generator())

    async def get_checkpoint(self, table_name: str) -> dict | None:
        return None

    def supports_incremental(self) -> bool:
        return True
