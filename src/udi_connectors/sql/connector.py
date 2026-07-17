import asyncio
import logging
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
from sqlalchemy import inspect, text
from sqlalchemy.engine import URL
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, create_async_engine
from sqlalchemy.orm import sessionmaker

from .config import SQLConfig

logger = logging.getLogger(__name__)


@Source("sql")
class SQLConnector:
    Config = SQLConfig

    DIALECT_MAP = {
        "postgresql": {"async": "postgresql+asyncpg", "sync": "postgresql+psycopg2"},
        "mysql": {"async": "mysql+asyncmy", "sync": "mysql+pymysql"},
        "mssql": {"async": "mssql+aioodbc", "sync": "mssql+pyodbc"},
        "oracle": {"async": "oracle+oracledb", "sync": "oracle+oracledb"},
        "sqlite": {"async": "sqlite+aiosqlite", "sync": "sqlite+pysqlite"},
    }

    def __init__(self, dialect: str = "postgresql", use_async: bool = True):
        self.dialect = dialect
        self.use_async = use_async
        self._engine: AsyncEngine | None = None
        self._async_session_factory = None

    async def connect(self, config: SQLConfig) -> None:
        logger.info("Connecting SQL: dialect=%s host=%s db=%s", config.dialect, config.host, config.database)
        try:
            driver = self.DIALECT_MAP[self.dialect]["async" if self.use_async else "sync"]
            if config.driver:
                driver = config.driver

            url = URL.create(
                drivername=driver,
                username=config.username,
                password=config.password.get_secret_value(),
                host=config.host,
                port=config.port,
                database=config.database,
                query=config.extra_params or {},
            )

            self._engine = create_async_engine(
                url,
                pool_size=config.pool_size,
                max_overflow=config.max_overflow,
                pool_timeout=config.pool_timeout,
                pool_pre_ping=True,
            )

            self._async_session_factory = sessionmaker(
                self._engine, class_=AsyncSession, expire_on_commit=False
            )

            async with self._engine.begin() as conn:
                await conn.execute(text("SELECT 1"))
            logger.info("Connected SQL: %s/%s", config.host, config.database)
        except Exception as e:
            raise ConnConnectionError(
                f"Failed to connect to {config.dialect}: {e}", "sql"
            ) from e

    async def disconnect(self) -> None:
        logger.info("Disconnecting SQL")
        if self._engine:
            await self._engine.dispose()

    async def test_connection(self) -> bool:
        try:
            if self._engine:
                async with self._engine.begin() as conn:
                    await conn.execute(text("SELECT 1"))
            return True
        except Exception:
            return False

    async def list_tables(self, config: SQLConfig) -> list[str]:
        def _list():
            with self._engine.sync_engine.connect() as conn:
                insp = inspect(conn)
                return insp.get_table_names()
        tables = await asyncio.get_running_loop().run_in_executor(None, _list)
        return sorted(tables)

    async def list_databases(self, config: SQLConfig) -> list[str]:
        dialect = config.dialect or "postgresql"
        if dialect == "postgresql":
            def _list():
                with self._engine.sync_engine.connect() as conn:
                    rows = conn.execute(text("SELECT datname FROM pg_database WHERE datistemplate = false ORDER BY datname"))
                    return [r[0] for r in rows]
        elif dialect == "mysql":
            def _list():
                with self._engine.sync_engine.connect() as conn:
                    rows = conn.execute(text("SHOW DATABASES"))
                    return [r[0] for r in rows]
        elif dialect == "mssql":
            def _list():
                with self._engine.sync_engine.connect() as conn:
                    rows = conn.execute(text("SELECT name FROM sys.databases ORDER BY name"))
                    return [r[0] for r in rows]
        else:
            return [config.database]
        return await asyncio.get_running_loop().run_in_executor(None, _list)

    async def get_schema(self, table_name: str) -> pa.Schema:
        async with self._engine.begin() as conn:
            def inspect_sync():
                insp = inspect(conn.sync_engine)
                return insp.get_columns(table_name)

            cols = await asyncio.get_running_loop().run_in_executor(None, inspect_sync)

        type_map = {
            "INTEGER": pa.int32(),
            "BIGINT": pa.int64(),
            "SMALLINT": pa.int16(),
            "NUMERIC": pa.decimal128(38, 10),
            "DECIMAL": pa.decimal128(38, 10),
            "REAL": pa.float32(),
            "FLOAT": pa.float64(),
            "DOUBLE": pa.float64(),
            "VARCHAR": pa.string(),
            "CHAR": pa.string(),
            "TEXT": pa.string(),
            "BOOLEAN": pa.bool_(),
            "DATE": pa.date32(),
            "TIMESTAMP": pa.timestamp("us"),
            "DATETIME": pa.timestamp("us"),
        }

        fields = []
        for col in cols:
            sql_type = str(col["type"]).upper().split("(")[0]
            arrow_type = type_map.get(sql_type, pa.string())
            fields.append(pa.field(col["name"], arrow_type, col.get("nullable", True)))
        return pa.schema(fields)

    async def extract(
        self,
        table_name: str,
        config: SQLConfig,
        columns: list[str] | None = None,
        filter_predicate: str | None = None,
    ) -> ExtractResult:
        logger.info("Extracting SQL table: %s", table_name)
        try:
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
            query = f"SELECT {col_list} FROM {table_name} {where_clause} {order_clause}"
            logger.debug("SQL query: %s", query)

            async def batch_generator() -> AsyncIterator[Batch]:
                async with self._async_session_factory() as session:
                    result = await session.stream(text(query))

                    batch_num = 0
                    async for partition in result.partitions(config.batch_size):
                        rows = list(partition)
                        if not rows:
                            break
                        df = pd.DataFrame(rows, columns=result.keys())
                        table = pa.Table.from_pandas(df)
                        logger.info("Batch %d: %d rows, %d bytes", batch_num, len(df), table.nbytes)
                        yield Batch(
                            data=table,
                            metadata=BatchMetadata(
                                source_name=self.dialect,
                                table_name=table_name,
                                batch_id=f"{table_name}_{batch_num}",
                                row_count=len(df),
                                byte_size=table.nbytes,
                                schema=table.schema,
                            ),
                        )
                        batch_num += 1

            return ExtractResult(batches=batch_generator())
        except Exception as e:
            raise ConnectorError(
                f"SQL extract failed for {table_name}: {e}", "sql", retryable=True
            ) from e

    async def get_checkpoint(self, table_name: str) -> dict | None:
        return None

    def supports_incremental(self) -> bool:
        return True
