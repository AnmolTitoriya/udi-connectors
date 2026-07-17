import sqlite3

import pytest
from udi_connectors.sql import SQLConfig, SQLConnector


@pytest.fixture
def sqlite_db(tmp_path):
    db_path = tmp_path / "test.db"
    conn = sqlite3.connect(str(db_path))
    conn.execute("CREATE TABLE widgets (id INTEGER PRIMARY KEY, name TEXT, price REAL)")
    conn.executemany(
        "INSERT INTO widgets (name, price) VALUES (?, ?)",
        [(f"widget_{i}", i * 1.5) for i in range(10)],
    )
    conn.commit()
    conn.close()
    return db_path


@pytest.fixture
def config(sqlite_db):
    return SQLConfig(
        dialect="sqlite",
        host="",
        port=0,
        database=str(sqlite_db),
        username="",
        password="",
        batch_size=100,
    )


@pytest.fixture
async def conn(config):
    c = SQLConnector()
    await c.connect(config)
    yield c
    await c.disconnect()


class TestSQLConnectorDialects:
    async def test_connect_honors_config_dialect(self, conn, config):
        # Regression test: SQLConnector() is always constructed with the default
        # dialect ("postgresql") by the registry; connect() must read config.dialect
        # rather than relying on the constructor default, or a sqlite connection
        # would try to speak postgresql.
        assert conn.dialect == "sqlite"
        assert await conn.test_connection()

    async def test_list_tables(self, conn, config):
        # Regression test: list_tables() used to call self._engine.sync_engine.connect()
        # inside run_in_executor, which breaks SQLAlchemy's async greenlet bridge
        # (MissingGreenlet) regardless of dialect.
        tables = await conn.list_tables(config)
        assert tables == ["widgets"]

    async def test_get_schema(self, conn, config):
        schema = await conn.get_schema("widgets")
        assert "id" in schema.names
        assert "name" in schema.names
        assert "price" in schema.names

    async def test_extract(self, conn, config):
        result = await conn.extract("widgets", config)
        batches = [b async for b in result.batches]
        total = sum(b.metadata.row_count for b in batches)
        assert total == 10
        assert all(b.metadata.source_name == "sqlite" for b in batches)

    async def test_supports_incremental(self):
        assert SQLConnector().supports_incremental()
