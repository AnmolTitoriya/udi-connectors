import asyncio
import logging
import platform
from collections.abc import AsyncIterator

import pandas as pd
import pyarrow as pa

from udi_connectors._registry import Source
from udi_packages import (
    Batch,
    BatchMetadata,
    CheckpointFile,
    ConnectorError,
    ExtractResult,
    ConnectionError as ConnConnectionError,
)

from .config import PostgreSQLConfig

logger = logging.getLogger(__name__)


def _fix_windows_event_loop():
    if platform.system() == "Windows":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())


@Source("postgresql")
class PostgreSQLConnector:
    Config = PostgreSQLConfig

    def __init__(self, use_async: bool = True):
        _fix_windows_event_loop()
        self.use_async = use_async
        self._pool = None
        self._async_pool = None

    async def connect(self, config: PostgreSQLConfig) -> None:
        logger.info("Connecting PostgreSQL: host=%s db=%s user=%s", config.host, config.database, config.username)
        try:
            if self.use_async:
                await self._connect_async(config)
            else:
                await self._connect_sync(config)
            logger.info("Connected PostgreSQL: %s/%s", config.host, config.database)
        except Exception as e:
            raise ConnConnectionError(
                f"Failed to connect to PostgreSQL: {e}", "postgresql"
            ) from e

    async def _connect_async(self, config: PostgreSQLConfig) -> None:
        import psycopg_pool
        self._async_pool = psycopg_pool.AsyncConnectionPool(
            conninfo=config.connection_string,
            min_size=config.pool_min_size,
            max_size=config.pool_max_size,
        )
        await self._async_pool.open()
        async with self._async_pool.connection() as conn:
            await conn.execute("SELECT 1")

    async def _connect_sync(self, config: PostgreSQLConfig) -> None:
        import psycopg_pool
        self._pool = psycopg_pool.ConnectionPool(
            conninfo=config.connection_string,
            min_size=config.pool_min_size,
            max_size=config.pool_max_size,
            open=True,
        )
        await asyncio.get_running_loop().run_in_executor(
            None, lambda: self._pool.getconn().execute("SELECT 1")
        )

    async def disconnect(self) -> None:
        logger.info("Disconnecting PostgreSQL")
        if self._async_pool:
            await self._async_pool.close()
        if self._pool:
            await asyncio.get_running_loop().run_in_executor(None, self._pool.close)

    async def test_connection(self) -> bool:
        try:
            if self.use_async and self._async_pool:
                async with self._async_pool.connection() as conn:
                    await conn.execute("SELECT 1")
            elif self._pool:
                await asyncio.get_running_loop().run_in_executor(
                    None, lambda: self._pool.getconn().execute("SELECT 1")
                )
            return True
        except Exception:
            return False

    async def list_tables(self, config: PostgreSQLConfig) -> list[str]:
        query = """
            SELECT table_name FROM information_schema.tables
            WHERE table_schema NOT IN ('pg_catalog', 'information_schema')
            ORDER BY table_name
        """
        if self.use_async:
            async with self._async_pool.connection() as conn, conn.cursor() as cur:
                await cur.execute(query)
                rows = await cur.fetchall()
        else:
            rows = await asyncio.get_running_loop().run_in_executor(
                None, self._list_tables_sync, query
            )
        return [r[0] for r in rows]

    def _list_tables_sync(self, query: str):
        with self._pool.connection() as conn, conn.cursor() as cur:
            cur.execute(query)
            return cur.fetchall()

    async def list_databases(self, config: PostgreSQLConfig) -> list[str]:
        query = "SELECT datname FROM pg_database WHERE datistemplate = false ORDER BY datname"
        if self.use_async:
            async with self._async_pool.connection() as conn, conn.cursor() as cur:
                await cur.execute(query)
                rows = await cur.fetchall()
        else:
            rows = await asyncio.get_running_loop().run_in_executor(
                None, self._list_tables_sync, query
            )
        return [r[0] for r in rows]

    async def get_schema(self, table_name: str) -> pa.Schema:
        query = """
            SELECT column_name, data_type, is_nullable
            FROM information_schema.columns
            WHERE table_name = %s
            ORDER BY ordinal_position
        """
        if self.use_async:
            async with self._async_pool.connection() as conn, conn.cursor() as cur:
                await cur.execute(query, (table_name,))
                rows = await cur.fetchall()
        else:
            rows = await asyncio.get_running_loop().run_in_executor(
                None, self._fetch_schema_sync, query, table_name
            )
        return self._rows_to_arrow_schema(rows)

    def _fetch_schema_sync(self, query: str, table_name: str):
        with self._pool.connection() as conn, conn.cursor() as cur:
            cur.execute(query, (table_name,))
            return cur.fetchall()

    def _rows_to_arrow_schema(self, rows) -> pa.Schema:
        type_map = {
            "integer": pa.int32(),
            "bigint": pa.int64(),
            "smallint": pa.int16(),
            "numeric": pa.decimal128(38, 10),
            "real": pa.float32(),
            "double precision": pa.float64(),
            "character varying": pa.string(),
            "varchar": pa.string(),
            "text": pa.string(),
            "boolean": pa.bool_(),
            "date": pa.date32(),
            "timestamp without time zone": pa.timestamp("us"),
            "timestamp with time zone": pa.timestamp("us", tz="UTC"),
            "uuid": pa.string(),
            "json": pa.string(),
            "jsonb": pa.string(),
        }
        fields = []
        for row in rows:
            col_name, pg_type, nullable = row
            arrow_type = type_map.get(pg_type.lower(), pa.string())
            fields.append(pa.field(col_name, arrow_type, nullable == "YES"))
        return pa.schema(fields)

    async def extract(
        self,
        table_name: str,
        config: PostgreSQLConfig,
        columns: list[str] | None = None,
        filter_predicate: str | None = None,
    ) -> ExtractResult:
        logger.info("Extracting PostgreSQL table: %s", table_name)
        try:
            if self.use_async:
                return await self._extract_async(table_name, config, columns, filter_predicate)
            return await self._extract_sync(table_name, config, columns, filter_predicate)
        except Exception as e:
            raise ConnectorError(
                f"PostgreSQL extract failed for {table_name}: {e}", "postgresql", retryable=True
            ) from e

    async def _extract_async(
        self,
        table_name: str,
        config: PostgreSQLConfig,
        columns: list[str] | None,
        filter_predicate: str | None,
    ) -> ExtractResult:
        col_list = ", ".join(columns) if columns else "*"
        cursor_name = config.cursor_name or f"export_{table_name}"

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

        batches: list[Batch] = []
        async with self._async_pool.connection() as conn:
            async with conn.cursor(name=cursor_name) as cur:
                await cur.execute(f"SELECT {col_list} FROM {table_name} {where_clause} {order_clause}")

                batch_num = 0
                while True:
                    rows = await cur.fetchmany(config.batch_size)
                    if not rows:
                        break
                    df = pd.DataFrame(rows, columns=[desc[0] for desc in cur.description])
                    table = pa.Table.from_pandas(df)
                    logger.info("Batch %d: %d rows, %d bytes", batch_num, len(df), table.nbytes)
                    batches.append(Batch(
                        data=table,
                        metadata=BatchMetadata(
                            source_name="postgresql",
                            table_name=table_name,
                            batch_id=f"{table_name}_{batch_num}",
                            row_count=len(df),
                            byte_size=table.nbytes,
                            schema=table.schema,
                        ),
                    ))
                    batch_num += 1

        logger.info("PostgreSQL extract complete: %d batches, %d rows", len(batches), sum(b.metadata.row_count for b in batches))
        async def batch_generator() -> AsyncIterator[Batch]:
            for batch in batches:
                yield batch

        return ExtractResult(batches=batch_generator())

    async def _extract_sync(
        self,
        table_name: str,
        config: PostgreSQLConfig,
        columns: list[str] | None,
        filter_predicate: str | None,
    ) -> ExtractResult:
        batches = await asyncio.get_running_loop().run_in_executor(
            None, self._extract_sync_impl, table_name, config, columns, filter_predicate
        )

        async def async_batch_generator() -> AsyncIterator[Batch]:
            for batch in batches:
                yield batch

        return ExtractResult(batches=async_batch_generator())

    def _extract_sync_impl(
        self,
        table_name: str,
        config: PostgreSQLConfig,
        columns: list[str] | None,
        filter_predicate: str | None,
    ) -> list[Batch]:
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

        batches: list[Batch] = []
        with self._pool.connection() as conn:
            with conn.cursor(name=f"export_{table_name}") as cur:
                cur.execute(f"SELECT {col_list} FROM {table_name} {where_clause} {order_clause}")

                batch_num = 0
                while True:
                    rows = cur.fetchmany(config.batch_size)
                    if not rows:
                        break
                    df = pd.DataFrame(rows, columns=[desc[0] for desc in cur.description])
                    table = pa.Table.from_pandas(df)
                    logger.info("Batch %d: %d rows, %d bytes", batch_num, len(df), table.nbytes)
                    batches.append(Batch(
                        data=table,
                        metadata=BatchMetadata(
                            source_name="postgresql",
                            table_name=table_name,
                            batch_id=f"{table_name}_{batch_num}",
                            row_count=len(df),
                            byte_size=table.nbytes,
                            schema=table.schema,
                        ),
                    ))
                    batch_num += 1

        logger.info("PostgreSQL sync extract complete: %d batches, %d rows", len(batches), sum(b.metadata.row_count for b in batches))
        return batches

    async def get_checkpoint(self, table_name: str) -> dict | None:
        return None

    def supports_incremental(self) -> bool:
        return True
