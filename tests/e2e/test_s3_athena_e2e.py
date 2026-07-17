"""Real-AWS end-to-end test: S3Connector dumps data, AthenaConnector reads it back.

Skipped unless E2E_S3_BUCKET is set. See tests/e2e/README.md for the full list
of required env vars and the minimal IAM policy needed to run this.
"""

import os
import time
import uuid

import boto3
import pandas as pd
import pyarrow as pa
import pytest
from udi_connectors.athena import AthenaConfig, AthenaConnector
from udi_connectors.s3 import S3Config, S3Connector
from udi_packages import Batch, BatchMetadata

pytestmark = pytest.mark.skipif(
    not os.environ.get("E2E_S3_BUCKET"),
    reason=(
        "Set E2E_S3_BUCKET (+ E2E_ATHENA_OUTPUT, optionally E2E_ATHENA_DATABASE/"
        "E2E_ATHENA_WORKGROUP/AWS_REGION) and real AWS credentials to run the "
        "S3 -> Athena e2e test. See tests/e2e/README.md."
    ),
)

REGION = os.environ.get("AWS_REGION", "us-east-1")
BUCKET = os.environ.get("E2E_S3_BUCKET")
DATABASE = os.environ.get("E2E_ATHENA_DATABASE", "udi_e2e")
WORKGROUP = os.environ.get("E2E_ATHENA_WORKGROUP", "primary")
OUTPUT_LOCATION = os.environ.get("E2E_ATHENA_OUTPUT")

SAMPLE_ROWS = [
    {"id": 1, "name": "alice", "active": True},
    {"id": 2, "name": "bob", "active": False},
    {"id": 3, "name": "carol", "active": True},
]


def _run_ddl(athena_client, query: str) -> None:
    kwargs = {
        "QueryString": query,
        "QueryExecutionContext": {"Database": DATABASE},
        "WorkGroup": WORKGROUP,
    }
    if OUTPUT_LOCATION:
        kwargs["ResultConfiguration"] = {"OutputLocation": OUTPUT_LOCATION}
    query_id = athena_client.start_query_execution(**kwargs)["QueryExecutionId"]

    for _ in range(60):
        status = athena_client.get_query_execution(QueryExecutionId=query_id)
        state = status["QueryExecution"]["Status"]["State"]
        if state == "SUCCEEDED":
            return
        if state in ("FAILED", "CANCELLED"):
            reason = status["QueryExecution"]["Status"].get("StateChangeReason", "unknown error")
            raise RuntimeError(f"DDL query failed ({state}): {reason}\nquery: {query}")
        time.sleep(1)

    raise TimeoutError(f"DDL query timed out after 60s:\nquery: {query}")


def _make_batch(table_name: str) -> Batch:
    df = pd.DataFrame(SAMPLE_ROWS)
    table = pa.Table.from_pandas(df, preserve_index=False)
    return Batch(
        data=table,
        metadata=BatchMetadata(
            source_name="e2e",
            table_name=table_name,
            batch_id=f"{table_name}_0",
            row_count=len(SAMPLE_ROWS),
            byte_size=table.nbytes,
            schema=table.schema,
        ),
    )


@pytest.fixture(scope="module")
def athena_client():
    return boto3.client("athena", region_name=REGION)


@pytest.fixture(scope="module")
def s3_client():
    return boto3.client("s3", region_name=REGION)


@pytest.fixture(scope="module")
def table_name() -> str:
    return f"udi_e2e_{uuid.uuid4().hex[:8]}"


@pytest.fixture(scope="module", autouse=True)
def _e2e_lifecycle(athena_client, s3_client, table_name):
    _run_ddl(athena_client, f"CREATE DATABASE IF NOT EXISTS {DATABASE}")

    yield

    try:
        _run_ddl(athena_client, f"DROP TABLE IF EXISTS {table_name}")
    except Exception:
        pass

    paginator = s3_client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=BUCKET, Prefix=f"{table_name}/"):
        objects = page.get("Contents", [])
        if objects:
            s3_client.delete_objects(
                Bucket=BUCKET, Delete={"Objects": [{"Key": o["Key"]} for o in objects]}
            )


class TestS3AthenaEndToEnd:
    async def test_dump_to_s3_then_query_via_athena(self, athena_client, table_name):
        s3_conn = S3Connector()
        await s3_conn.connect(
            S3Config(bucket_name=BUCKET, region=REGION, file_format="parquet", compression="snappy")
        )

        batch = _make_batch(table_name)

        async def batch_gen():
            yield batch

        load_result = await s3_conn.load(batch_gen(), table_name)
        await s3_conn.disconnect()

        assert load_result.rows_loaded == len(SAMPLE_ROWS)
        assert load_result.batch_count == 1
        assert not load_result.errors

        _run_ddl(
            athena_client,
            f"CREATE EXTERNAL TABLE {table_name} (\n"
            f"id BIGINT,\n"
            f"name STRING,\n"
            f"active BOOLEAN\n"
            f")\n"
            f"STORED AS PARQUET\n"
            f"LOCATION 's3://{BUCKET}/{table_name}/'",
        )

        athena_config = AthenaConfig(
            region=REGION,
            database=DATABASE,
            workgroup=WORKGROUP,
            output_location=OUTPUT_LOCATION,
        )
        athena_conn = AthenaConnector()
        await athena_conn.connect(athena_config)
        assert await athena_conn.test_connection()

        result = await athena_conn.extract(table_name, athena_config)
        batches = [b async for b in result.batches]
        await athena_conn.disconnect()

        assert sum(b.metadata.row_count for b in batches) == len(SAMPLE_ROWS)

        got_by_id = {row["id"]: row for b in batches for row in b.data.to_pylist()}
        for expected in SAMPLE_ROWS:
            actual = got_by_id[expected["id"]]
            assert actual["name"] == expected["name"]
            assert actual["active"] == expected["active"]
