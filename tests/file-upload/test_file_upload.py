import pytest
from udi_connectors.file_upload import FileUploadConfig, FileUploadConnector


class TestFileUploadConnector:
    async def test_connect(self, tmp_path):
        config = FileUploadConfig(input_dir=str(tmp_path))
        conn = FileUploadConnector()
        await conn.connect(config)
        assert await conn.test_connection()
        await conn.disconnect()

    async def test_connect_nonexistent_dir(self):
        config = FileUploadConfig(input_dir="/nonexistent/path")
        conn = FileUploadConnector()
        with pytest.raises(Exception, match="does not exist"):
            await conn.connect(config)

    async def test_extract_empty_dir(self, tmp_path):
        config = FileUploadConfig(input_dir=str(tmp_path))
        conn = FileUploadConnector()
        await conn.connect(config)
        result = await conn.extract(config=config)
        batches = [b async for b in result.batches]
        assert len(batches) == 0
        await conn.disconnect()

    async def test_extract_files(self, tmp_path):
        (tmp_path / "file1.txt").write_text("hello")
        (tmp_path / "file2.csv").write_text("a,b,c\n1,2,3")
        (tmp_path / "data.json").write_text('{"x": 1}')

        config = FileUploadConfig(input_dir=str(tmp_path), batch_size=10)
        conn = FileUploadConnector()
        await conn.connect(config)
        result = await conn.extract(config=config)

        batches = [b async for b in result.batches]
        assert len(batches) == 1
        assert batches[0].metadata.row_count == 3
        assert batches[0].data.column_names == ["filename", "size", "type", "content"]
        # verify content is base64-encoded
        content = batches[0].data.column("content")[0].as_py()
        assert isinstance(content, str)
        assert len(content) > 0
        await conn.disconnect()

    async def test_extract_with_pattern(self, tmp_path):
        (tmp_path / "file1.txt").write_text("a")
        (tmp_path / "file2.csv").write_text("x,y\n1,2")
        (tmp_path / "ignore.log").write_text("skip")

        config = FileUploadConfig(input_dir=str(tmp_path), file_pattern="*.csv")
        conn = FileUploadConnector()
        await conn.connect(config)
        result = await conn.extract(config=config)

        batches = [b async for b in result.batches]
        assert len(batches) == 1
        assert batches[0].metadata.row_count == 1
        assert batches[0].data.column("filename")[0].as_py() == "file2.csv"
        await conn.disconnect()

    async def test_extract_multiple_batches(self, tmp_path):
        for i in range(50):
            (tmp_path / f"file_{i}.txt").write_text(f"content {i}")

        config = FileUploadConfig(input_dir=str(tmp_path), batch_size=20)
        conn = FileUploadConnector()
        await conn.connect(config)
        result = await conn.extract(config=config)

        batches = [b async for b in result.batches]
        assert len(batches) == 3
        assert sum(b.metadata.row_count for b in batches) == 50
        await conn.disconnect()

    async def test_get_schema(self, tmp_path):
        config = FileUploadConfig(input_dir=str(tmp_path))
        conn = FileUploadConnector()
        await conn.connect(config)
        schema = await conn.get_schema()
        assert schema.field("filename") is not None
        assert schema.field("size") is not None
        assert schema.field("type") is not None
        assert schema.field("content") is not None
        await conn.disconnect()

    async def test_supports_incremental(self):
        conn = FileUploadConnector()
        assert not conn.supports_incremental()
