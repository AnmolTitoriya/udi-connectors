import asyncio
import logging
from collections.abc import AsyncIterator
from pathlib import Path

import pandas as pd
import pyarrow as pa
from udi_connectors._registry import Source
from udi_packages import (
    Batch,
    BatchMetadata,
    CheckpointFile,
    ConnectorError,
    ExtractResult,
)

from .config import FileUploadConfig

logger = logging.getLogger(__name__)


TEXT_EXTENSIONS = frozenset({
    ".txt", ".csv", ".json", ".xml", ".md", ".log",
    ".py", ".js", ".ts", ".html", ".css", ".yaml", ".yml",
    ".toml", ".ini", ".cfg", ".env",
    ".sh", ".bat", ".ps1",
    ".sql", ".rb", ".java", ".kt", ".scala",
    ".c", ".cpp", ".h", ".hpp", ".rs", ".go", ".php",
    ".r", ".swift", ".lua", ".pl", ".pm",
    ".rake", ".gemfile",
    ".dockerfile", ".gitignore", ".editorconfig",
    ".json5", ".hjson",
    ".makefile", ".cmake",
    ".gradle", ".sbt",
    ".erl", ".ex", ".exs",
    ".clj", ".cljs", ".edn",
    ".fs", ".fsx",
    ".nim", ".zig",
    ".tex", ".sty", ".cls",
    ".rst", ".adoc", ".asciidoc",
})


@Source("file_upload")
class FileUploadConnector:
    Config = FileUploadConfig

    def __init__(self):
        self._config: FileUploadConfig | None = None

    async def connect(self, config: FileUploadConfig) -> None:
        logger.info("Connecting file_upload: input_dir=%s pattern=%s files=%s", config.input_dir, config.file_pattern, config.files)
        self._config = config
        folder = Path(config.input_dir)
        if not folder.exists():
            raise ConnectorError(
                f"Input directory does not exist: {config.input_dir}",
                "file_upload",
                retryable=False,
            )
        if not folder.is_dir():
            raise ConnectorError(
                f"Path is not a directory: {config.input_dir}",
                "file_upload",
                retryable=False,
            )
        logger.info("Connected file_upload: %s", config.input_dir)

    async def disconnect(self) -> None:
        logger.info("Disconnecting file_upload")

    async def test_connection(self) -> bool:
        if not self._config:
            return False
        folder = Path(self._config.input_dir)
        return folder.exists() and folder.is_dir()

    async def list_tables(self, config: FileUploadConfig) -> list[str]:
        return ["files"]

    async def list_databases(self, config: FileUploadConfig) -> list[str]:
        return ["files"]

    async def get_schema(self, _table_name: str = "") -> pa.Schema:
        fields = [
            pa.field("filename", pa.string()),
            pa.field("size", pa.int64()),
            pa.field("type", pa.string()),
        ]
        if self._config and self._config.include_content:
            fields.append(pa.field("content", pa.string()))
        return pa.schema(fields)

    @staticmethod
    def _read_text(f: Path) -> str:
        return f.read_text("utf-8")

    @staticmethod
    def _extract_pdf_text(f: Path) -> str | None:
        try:
            import pdfplumber
            with pdfplumber.open(f) as pdf:
                pages = [p.extract_text() for p in pdf.pages if p.extract_text()]
                return "\n\n".join(pages) if pages else None
        except Exception:
            return None

    @staticmethod
    def _extract_docx_text(f: Path) -> str | None:
        try:
            from docx import Document
            doc = Document(str(f))
            paragraphs = [p.text for p in doc.paragraphs if p.text.strip()]
            return "\n\n".join(paragraphs) if paragraphs else None
        except Exception:
            return None

    async def extract(
        self,
        _table_name: str = "",
        config: FileUploadConfig | None = None,
        columns: list[str] | None = None,
        filter_predicate: str | None = None,
    ) -> ExtractResult:
        cfg = config or self._config
        if not cfg:
            raise ConnectorError("FileUploadConnector is not connected", "file_upload", retryable=False)

        logger.info("Extracting files from: %s", cfg.input_dir)
        folder = Path(cfg.input_dir)

        loop = asyncio.get_running_loop()

        if cfg.files:
            candidates = [folder / Path(f) for f in cfg.files]
            def filter_files(paths):
                return [p for p in paths if p.is_file()]
            files = await loop.run_in_executor(None, filter_files, candidates)
            logger.info("Filtered %d specific files", len(files))
        else:
            pattern = cfg.file_pattern
            def scan_files():
                if cfg.recursive:
                    return list(folder.rglob(pattern))
                return list(folder.glob(pattern))
            files = await loop.run_in_executor(None, scan_files)
            files = [f for f in files if f.is_file()]
            logger.info("Scanned %d files with pattern=%s", len(files), pattern)

        files.sort()

        if cfg.checkpoint_file:
            cp = CheckpointFile(cfg.checkpoint_file)
            processed = cp.get(_table_name or folder.name)
            if processed and isinstance(processed, list):
                processed_set = set(processed)
                before = len(files)
                files = [f for f in files if str(f.relative_to(folder)) not in processed_set]
                skipped = before - len(files)
                if skipped:
                    logger.info("Skipping %d already-processed files from checkpoint", skipped)

        if not files:
            logger.warning("No files found in: %s", cfg.input_dir)
            async def empty_gen() -> AsyncIterator[Batch]:
                return
                yield
            return ExtractResult(batches=empty_gen())

        logger.info("Processing %d files in batches of %d", len(files), cfg.batch_size)
        async def batch_generator() -> AsyncIterator[Batch]:
            for batch_num, i in enumerate(range(0, len(files), cfg.batch_size)):
                chunk = files[i:i + cfg.batch_size]
                rows = []
                for f in chunk:
                    stat = await loop.run_in_executor(None, lambda f=f: f.stat())
                    row = {
                        "filename": str(f.relative_to(folder)),
                        "size": stat.st_size,
                        "type": f.suffix.lower() or "unknown",
                    }
                    if cfg.include_content:
                        suffix = f.suffix.lower()
                        if suffix in TEXT_EXTENSIONS:
                            row["content"] = await loop.run_in_executor(
                                None, self._read_text, f
                            )
                        elif suffix == ".pdf":
                            row["content"] = await loop.run_in_executor(
                                None, self._extract_pdf_text, f
                            )
                        elif suffix == ".docx":
                            row["content"] = await loop.run_in_executor(
                                None, self._extract_docx_text, f
                            )
                        else:
                            row["content"] = None
                    rows.append(row)

                df = pd.DataFrame(rows)
                table = pa.Table.from_pandas(df)

                logger.info("Batch %d: %d files, %d bytes", batch_num, len(rows), table.nbytes)
                yield Batch(
                    data=table,
                    metadata=BatchMetadata(
                        source_name="file_upload",
                        table_name=folder.name,
                        batch_id=f"files_{batch_num}",
                        row_count=len(rows),
                        byte_size=table.nbytes,
                        schema=table.schema,
                    ),
                )

        logger.info("File extraction complete: %d files", len(files))
        return ExtractResult(batches=batch_generator())

    def supports_incremental(self) -> bool:
        return False
