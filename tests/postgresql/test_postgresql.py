import os
import platform

import pytest
from dotenv import load_dotenv
from udi_connectors.postgresql import PostgreSQLConfig, PostgreSQLConnector

load_dotenv()

pytestmark = pytest.mark.skipif(
    not os.environ.get("TEST_PG"),
    reason="Set TEST_PG=1 and run: docker compose up -d postgres"
)


async def _pg_exec(pg_config: PostgreSQLConfig, sql: str):
    import psycopg
    async with await psycopg.AsyncConnection.connect(
        pg_config.connection_string
    ) as conn:
        await conn.execute(sql)


async def _pg_executemany(pg_config: PostgreSQLConfig, sql: str, params: list[tuple]):
    import psycopg
    async with await psycopg.AsyncConnection.connect(
        pg_config.connection_string
    ) as conn:
        async with conn.cursor() as cur:
            await cur.executemany(sql, params)


@pytest.fixture
def pg_config():
    return PostgreSQLConfig(
        host=os.getenv("PG_HOST", "localhost"),
        port=int(os.getenv("PG_PORT", "5432")),
        database=os.getenv("PG_DATABASE", "testdb"),
        username=os.getenv("PG_USERNAME", "test"),
        password=os.getenv("PG_PASSWORD", "test"),
        ssl_mode=os.getenv("PG_SSLMODE", "prefer"),
        batch_size=100,
    )


@pytest.fixture
async def seeded_db(pg_config):
    await _pg_exec(pg_config, """
        CREATE TABLE IF NOT EXISTS test_users (
            id SERIAL PRIMARY KEY,
            name TEXT NOT NULL,
            email TEXT
        )
    """)
    await _pg_executemany(
        pg_config,
        "INSERT INTO test_users (name, email) VALUES (%s, %s)",
        [(f"user_{i}", f"user_{i}@test.com") for i in range(50)],
    )
    yield
    await _pg_exec(pg_config, "DROP TABLE IF EXISTS test_users")


class TestPostgreSQLConnector:
    async def test_connect(self, pg_config):
        pg = PostgreSQLConnector(use_async=True)
        await pg.connect(pg_config)
        assert pg._async_pool is not None
        await pg.disconnect()

    async def test_test_connection(self, pg_config):
        pg = PostgreSQLConnector(use_async=True)
        await pg.connect(pg_config)
        assert await pg.test_connection()
        await pg.disconnect()

    async def test_get_schema(self, pg_config, seeded_db):
        pg = PostgreSQLConnector(use_async=True)
        await pg.connect(pg_config)
        schema = await pg.get_schema("test_users")
        assert schema.field("id") is not None
        assert schema.field("name") is not None
        await pg.disconnect()

    async def test_extract(self, pg_config, seeded_db):
        pg = PostgreSQLConnector(use_async=True)
        await pg.connect(pg_config)
        result = await pg.extract("test_users", pg_config)

        batches = [b async for b in result.batches]

        assert len(batches) >= 1
        total = sum(b.metadata.row_count for b in batches)
        assert total == 50
        await pg.disconnect()

    async def test_extract_with_columns(self, pg_config, seeded_db):
        pg = PostgreSQLConnector(use_async=True)
        await pg.connect(pg_config)
        result = await pg.extract("test_users", pg_config, columns=["id", "name"])

        batches = []
        async for b in result.batches:
            batches.append(b)

        assert batches[0].data.num_columns == 2
        await pg.disconnect()

    async def test_supports_incremental(self):
        pg = PostgreSQLConnector()
        assert pg.supports_incremental()
