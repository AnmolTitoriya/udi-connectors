import json
from pathlib import Path

import pyarrow as pa
import pytest
from udi_connectors import migrate_all
from udi_packages import Batch, BatchMetadata, CheckpointFile, ExtractResult


@pytest.fixture
def sample_batches():
    import pandas as pd
    def _make(rows, batch_id=0):
        df = pd.DataFrame({"id": range(rows), "name": [f"n_{i}" for i in range(rows)]})
        table = pa.Table.from_pandas(df)
        return Batch(
            data=table,
            metadata=BatchMetadata(
                source_name="test", table_name="users",
                batch_id=f"users_{batch_id}", row_count=rows,
                byte_size=table.nbytes, schema=table.schema,
            ),
        )
    return _make


@pytest.fixture(autouse=True)
def _mock_pg_connect(monkeypatch):
    """PostgreSQL connect requires libpq (not installed on this machine)."""
    async def fake_connect(self, config):
        pass
    async def fake_disconnect(self):
        pass
    monkeypatch.setattr("udi_connectors.postgresql.connector.PostgreSQLConnector.connect", fake_connect)
    monkeypatch.setattr("udi_connectors.postgresql.connector.PostgreSQLConnector.disconnect", fake_disconnect)


class TestMigrateErrors:
    async def test_invalid_source(self):
        with pytest.raises(ValueError, match="Unknown source"):
            await migrate_all("nonexistent", "s3", tables=["users"],
                              target_kwargs={"bucket_name": "b"})

    async def test_invalid_target(self):
        with pytest.raises(ValueError, match="Unknown target"):
            await migrate_all("postgresql", "nonexistent", tables=["users"],
                              source_kwargs={"host": "x", "database": "x", "username": "x", "password": "x"})


class TestMigratePostgresToS3:
    async def test_single_table(self, sample_batches, monkeypatch):
        pytest.importorskip("moto")
        from moto import mock_aws
        with mock_aws():
            import boto3
            boto3.client("s3", region_name="us-east-1").create_bucket(Bucket="test-bucket")

            async def fake_extract(self, table_name, config, columns=None, filter_predicate=None):
                async def gen():
                    yield sample_batches(10)
                return ExtractResult(batches=gen())

            monkeypatch.setattr("udi_connectors.postgresql.connector.PostgreSQLConnector.extract", fake_extract)

            results = await migrate_all(
                "postgresql", "s3", tables=["users"],
                source_kwargs={"host": "h", "database": "d", "username": "u", "password": "p"},
                target_kwargs={"bucket_name": "test-bucket", "region": "us-east-1"},
            )
            assert len(results) == 1
            assert results[0].rows_loaded == 10
            assert results[0].batch_count == 1

    async def test_multiple_tables(self, sample_batches, monkeypatch):
        pytest.importorskip("moto")
        from moto import mock_aws
        with mock_aws():
            import boto3
            boto3.client("s3", region_name="us-east-1").create_bucket(Bucket="test-bucket")

            extract_counts = {"users": 10, "orders": 5}

            async def fake_extract(self, table_name, config, columns=None, filter_predicate=None):
                count = extract_counts.get(table_name, 0)
                async def gen():
                    if count > 0:
                        yield sample_batches(count)
                return ExtractResult(batches=gen())

            monkeypatch.setattr("udi_connectors.postgresql.connector.PostgreSQLConnector.extract", fake_extract)

            results = await migrate_all(
                "postgresql", "s3", tables=["users", "orders"],
                source_kwargs={"host": "h", "database": "d", "username": "u", "password": "p"},
                target_kwargs={"bucket_name": "test-bucket", "region": "us-east-1"},
            )
            assert len(results) == 2
            assert results[0].table_name == "users"
            assert results[0].rows_loaded == 10
            assert results[1].table_name == "orders"
            assert results[1].rows_loaded == 5


class TestMigrateFileUploadToS3:
    async def test_single_table(self, tmp_path):
        pytest.importorskip("moto")
        (tmp_path / "file1.txt").write_text("hello world")
        (tmp_path / "file2.csv").write_text("a,b\n1,2")

        from moto import mock_aws
        with mock_aws():
            import boto3
            boto3.client("s3", region_name="us-east-1").create_bucket(Bucket="test-bucket")

            results = await migrate_all(
                "file_upload", "s3", tables=["docs"],
                source_kwargs={"input_dir": str(tmp_path)},
                target_kwargs={"bucket_name": "test-bucket", "region": "us-east-1"},
            )
            assert len(results) == 1
            assert results[0].rows_loaded == 2
            assert results[0].batch_count == 1

    async def test_multiple_batches(self, tmp_path):
        pytest.importorskip("moto")
        for i in range(25):
            (tmp_path / f"f_{i}.txt").write_text(f"content {i}")

        from moto import mock_aws
        with mock_aws():
            import boto3
            boto3.client("s3", region_name="us-east-1").create_bucket(Bucket="test-bucket")

            results = await migrate_all(
                "file_upload", "s3", tables=["files"],
                source_kwargs={"input_dir": str(tmp_path), "batch_size": 10},
                target_kwargs={"bucket_name": "test-bucket", "region": "us-east-1"},
            )
            assert len(results) == 1
            assert results[0].rows_loaded == 25
            assert results[0].batch_count == 3


class TestRetryAndCheckpoint:
    async def test_retry_on_extract_failure(self, sample_batches, monkeypatch):
        """Extract fails on first attempt, succeeds on retry."""
        pytest.importorskip("moto")
        from moto import mock_aws
        with mock_aws():
            import boto3
            boto3.client("s3", region_name="us-east-1").create_bucket(Bucket="test-bucket")

            call_count = 0

            async def fake_extract(self, table_name, config, columns=None, filter_predicate=None):
                nonlocal call_count
                call_count += 1
                if call_count == 1:
                    raise Exception("First attempt failed")
                async def gen():
                    yield sample_batches(10)
                return ExtractResult(batches=gen())

            monkeypatch.setattr("udi_connectors.postgresql.connector.PostgreSQLConnector.extract", fake_extract)

            results = await migrate_all(
                "postgresql", "s3", tables=["users"],
                source_kwargs={"host": "h", "database": "d", "username": "u", "password": "p"},
                target_kwargs={"bucket_name": "test-bucket", "region": "us-east-1"},
            )
            assert len(results) == 1
            assert results[0].rows_loaded == 10
            assert call_count == 2

    async def test_retry_all_three_fail(self, sample_batches, monkeypatch):
        """All 3 retries fail — exception propagates."""
        pytest.importorskip("moto")
        from moto import mock_aws
        with mock_aws():
            import boto3
            boto3.client("s3", region_name="us-east-1").create_bucket(Bucket="test-bucket")

            call_count = 0

            async def fake_extract(self, table_name, config, columns=None, filter_predicate=None):
                nonlocal call_count
                call_count += 1
                raise Exception(f"Attempt {call_count} failed")

            monkeypatch.setattr("udi_connectors.postgresql.connector.PostgreSQLConnector.extract", fake_extract)

            with pytest.raises(Exception, match="Attempt 3 failed"):
                await migrate_all(
                    "postgresql", "s3", tables=["users"],
                    source_kwargs={"host": "h", "database": "d", "username": "u", "password": "p"},
                    target_kwargs={"bucket_name": "test-bucket", "region": "us-east-1"},
                )
            assert call_count == 3

    async def test_file_upload_checkpoint_skips_processed(self, tmp_path):
        """Files in checkpoint are skipped on next run."""
        pytest.importorskip("moto")
        input_dir = tmp_path / "input"
        input_dir.mkdir()
        (input_dir / "file1.txt").write_text("hello")
        (input_dir / "file2.txt").write_text("world")
        (input_dir / "file3.txt").write_text("new file")

        cp_path = tmp_path / "checkpoint.json"
        cp_path.write_text(json.dumps({"docs": ["file1.txt", "file2.txt"]}))

        from moto import mock_aws
        with mock_aws():
            import boto3
            boto3.client("s3", region_name="us-east-1").create_bucket(Bucket="test-bucket")

            results = await migrate_all(
                "file_upload", "s3", tables=["docs"],
                source_kwargs={
                    "input_dir": str(input_dir),
                    "checkpoint_file": str(cp_path),
                },
                target_kwargs={"bucket_name": "test-bucket", "region": "us-east-1"},
            )
            assert len(results) == 1
            assert results[0].rows_loaded == 1  # only file3.txt
            assert results[0].batch_count == 1
