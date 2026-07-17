from unittest.mock import MagicMock

import pyarrow as pa
import pytest
from botocore.exceptions import ClientError
from udi_connectors.athena import AthenaConfig, AthenaConnector
from udi_packages import ConnectorError
from udi_packages import ConnectionError as ConnConnectionError


def _mock_client(mocker) -> MagicMock:
    client = MagicMock()
    mocker.patch("udi_connectors.athena.connector.boto3.client", return_value=client)
    return client


@pytest.fixture
def config():
    return AthenaConfig(database="analytics", workgroup="primary", output_location="s3://bucket/results/")


class TestAthenaConnector:
    async def test_connect_calls_get_work_group(self, mocker, config):
        client = _mock_client(mocker)
        conn = AthenaConnector()
        await conn.connect(config)
        client.get_work_group.assert_called_once_with(WorkGroup="primary")
        await conn.disconnect()

    async def test_connect_wraps_client_error(self, mocker, config):
        client = _mock_client(mocker)
        client.get_work_group.side_effect = ClientError(
            {"Error": {"Code": "InvalidRequestException", "Message": "no such workgroup"}}, "GetWorkGroup"
        )
        conn = AthenaConnector()
        with pytest.raises(ConnConnectionError):
            await conn.connect(config)

    async def test_test_connection(self, mocker, config):
        _mock_client(mocker)
        conn = AthenaConnector()
        await conn.connect(config)
        assert await conn.test_connection()

    async def test_list_databases(self, mocker, config):
        client = _mock_client(mocker)
        paginator = MagicMock()
        paginator.paginate.return_value = [{"DatabaseList": [{"Name": "b"}, {"Name": "a"}]}]
        client.get_paginator.return_value = paginator
        conn = AthenaConnector()
        await conn.connect(config)
        assert await conn.list_databases(config) == ["a", "b"]

    async def test_list_tables(self, mocker, config):
        client = _mock_client(mocker)
        paginator = MagicMock()
        paginator.paginate.return_value = [{"TableMetadataList": [{"Name": "orders"}, {"Name": "customers"}]}]
        client.get_paginator.return_value = paginator
        conn = AthenaConnector()
        await conn.connect(config)
        assert await conn.list_tables(config) == ["customers", "orders"]

    async def test_get_schema(self, mocker, config):
        client = _mock_client(mocker)
        client.get_table_metadata.return_value = {
            "TableMetadata": {
                "Columns": [
                    {"Name": "id", "Type": "bigint"},
                    {"Name": "name", "Type": "varchar(255)"},
                    {"Name": "active", "Type": "boolean"},
                ]
            }
        }
        conn = AthenaConnector()
        await conn.connect(config)
        schema = await conn.get_schema("users")
        assert schema.field("id").type == pa.int64()
        assert schema.field("name").type == pa.string()
        assert schema.field("active").type == pa.bool_()

    async def test_extract_single_page(self, mocker, config):
        client = _mock_client(mocker)
        client.start_query_execution.return_value = {"QueryExecutionId": "q1"}
        client.get_query_execution.return_value = {"QueryExecution": {"Status": {"State": "SUCCEEDED"}}}
        column_info = [{"Name": "id", "Type": "integer"}, {"Name": "name", "Type": "varchar"}]
        client.get_query_results.return_value = {
            "ResultSet": {
                "ResultSetMetadata": {"ColumnInfo": column_info},
                "Rows": [
                    {"Data": [{"VarCharValue": "id"}, {"VarCharValue": "name"}]},
                    {"Data": [{"VarCharValue": "1"}, {"VarCharValue": "alice"}]},
                    {"Data": [{"VarCharValue": "2"}, {"VarCharValue": "bob"}]},
                ],
            }
        }
        conn = AthenaConnector()
        await conn.connect(config)
        result = await conn.extract("users", config)
        batches = [b async for b in result.batches]
        assert len(batches) == 1
        assert batches[0].metadata.row_count == 2
        assert batches[0].data.column("name").to_pylist() == ["alice", "bob"]
        assert batches[0].data.column("id").to_pylist() == [1, 2]

    async def test_extract_paginated_batches_respect_batch_size(self, mocker, config):
        client = _mock_client(mocker)
        client.start_query_execution.return_value = {"QueryExecutionId": "q1"}
        client.get_query_execution.return_value = {"QueryExecution": {"Status": {"State": "SUCCEEDED"}}}
        column_info = [{"Name": "id", "Type": "integer"}]

        header_row = {"Data": [{"VarCharValue": "id"}]}
        page1_rows = [{"Data": [{"VarCharValue": str(i)}]} for i in range(3)]
        page2_rows = [{"Data": [{"VarCharValue": str(i)}]} for i in range(3, 5)]

        client.get_query_results.side_effect = [
            {
                "ResultSet": {"ResultSetMetadata": {"ColumnInfo": column_info}, "Rows": [header_row, *page1_rows]},
                "NextToken": "tok",
            },
            {"ResultSet": {"ResultSetMetadata": {"ColumnInfo": column_info}, "Rows": page2_rows}},
        ]

        config.batch_size = 2
        conn = AthenaConnector()
        await conn.connect(config)
        result = await conn.extract("nums", config)
        batches = [b async for b in result.batches]
        assert sum(b.metadata.row_count for b in batches) == 5
        assert all(b.metadata.row_count <= 2 for b in batches)

    async def test_query_failure_raises(self, mocker, config):
        client = _mock_client(mocker)
        client.start_query_execution.return_value = {"QueryExecutionId": "q1"}
        client.get_query_execution.return_value = {
            "QueryExecution": {"Status": {"State": "FAILED", "StateChangeReason": "syntax error"}}
        }
        conn = AthenaConnector()
        await conn.connect(config)
        with pytest.raises(ConnectorError):
            await conn.extract("users", config)

    async def test_supports_incremental(self):
        assert AthenaConnector().supports_incremental()
