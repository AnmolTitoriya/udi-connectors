import pyarrow as pa
import pytest
from udi_packages import Batch, BatchMetadata
from udi_connectors.s3 import S3Config, S3Connector

pytest.importorskip("moto")


@pytest.fixture(autouse=True)
def _aws_ctx():
    from moto import mock_aws
    with mock_aws():
        import boto3
        boto3.client("s3", region_name="us-east-1").create_bucket(Bucket="test-bucket")
        yield


@pytest.fixture
async def conn(s3_bucket, s3_region):
    c = S3Connector()
    await c.connect(S3Config(bucket_name=s3_bucket, region=s3_region))
    yield c
    await c.disconnect()


def _make_batch(table_name: str, rows: int, batch_id: int = 0) -> Batch:
    import pandas as pd
    data = {"id": range(rows), "name": [f"n_{i}" for i in range(rows)]}
    df = pd.DataFrame(data)
    table = pa.Table.from_pandas(df)
    return Batch(
        data=table,
        metadata=BatchMetadata(
            source_name="test",
            table_name=table_name,
            batch_id=f"{table_name}_{batch_id}",
            row_count=rows,
            byte_size=table.nbytes,
            schema=table.schema,
        ),
    )


class TestS3Connector:
    async def test_connect(self, conn):
        assert conn._client is not None

    async def test_test_connection(self, conn):
        assert await conn.test_connection()

    async def test_load_single_batch(self, conn, s3_bucket):
        batch = _make_batch("users", 10)

        async def batch_gen():
            yield batch

        result = await conn.load(batch_gen(), "users")
        assert result.rows_loaded == 10
        assert result.batch_count == 1
        assert len(result.errors) == 0

    async def test_load_multiple_batches(self, conn, s3_bucket):
        async def batch_gen():
            for i in range(3):
                yield _make_batch("orders", 5, i)

        result = await conn.load(batch_gen(), "orders")
        assert result.rows_loaded == 15
        assert result.batch_count == 3
        assert len(result.errors) == 0

    async def test_load_empty_batches(self, conn, s3_bucket):
        async def empty_gen():
            return
            yield  # pragma: no cover

        result = await conn.load(empty_gen(), "empty")
        assert result.rows_loaded == 0
        assert result.batch_count == 0
