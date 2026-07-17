import os

import pytest
from udi_connectors.mongodb import MongoDBConfig, MongoDBConnector

pytestmark = pytest.mark.skipif(
    not os.environ.get("TEST_MONGO"),
    reason="Set TEST_MONGO=1 and run: docker compose up -d mongodb"
)


@pytest.fixture
def mongo_config():
    return MongoDBConfig(
        connection_string=os.getenv("MONGO_URL", "mongodb://localhost:27017"),
        database="testdb",
        collection="test_collection",
        batch_size=100,
    )


@pytest.fixture
async def seeded_collection(mongo_config):
    from motor.motor_asyncio import AsyncIOMotorClient
    client = AsyncIOMotorClient(mongo_config.connection_string)
    db = client[mongo_config.database]
    col = db[mongo_config.collection]
    await col.delete_many({})
    await col.insert_many([
        {"_id": i, "name": f"doc_{i}", "value": i * 10}
        for i in range(50)
    ])
    yield
    await col.drop()
    client.close()


class TestMongoDBConnector:
    async def test_connect(self, mongo_config):
        m = MongoDBConnector(use_async=True)
        await m.connect(mongo_config)
        assert m._async_client is not None
        await m.disconnect()

    async def test_test_connection(self, mongo_config):
        m = MongoDBConnector(use_async=True)
        await m.connect(mongo_config)
        assert await m.test_connection()
        await m.disconnect()

    async def test_get_schema(self, mongo_config, seeded_collection):
        m = MongoDBConnector(use_async=True)
        await m.connect(mongo_config)
        schema = await m.get_schema(mongo_config.collection)
        assert len(schema) > 0
        await m.disconnect()

    async def test_extract(self, mongo_config, seeded_collection):
        m = MongoDBConnector(use_async=True)
        await m.connect(mongo_config)
        result = await m.extract(mongo_config.collection, mongo_config)

        batches = [b async for b in result.batches]
        assert len(batches) >= 1
        total = sum(b.metadata.row_count for b in batches)
        assert total == 50
        await m.disconnect()

    async def test_extract_with_filter(self, mongo_config, seeded_collection):
        m = MongoDBConnector(use_async=True)
        await m.connect(mongo_config)
        result = await m.extract(
            mongo_config.collection, mongo_config,
            filter_dict={"value": {"$gte": 200}},
        )

        batches = [b async for b in result.batches]
        total = sum(b.metadata.row_count for b in batches)
        assert total == 30  # docs 20-49 have value >= 200
        await m.disconnect()

    async def test_supports_incremental(self):
        m = MongoDBConnector()
        assert m.supports_incremental()
