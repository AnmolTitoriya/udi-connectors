import asyncio
import logging
from collections.abc import AsyncIterator

import motor.motor_asyncio
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
from pymongo import MongoClient

from .config import MongoDBConfig

logger = logging.getLogger(__name__)


@Source("mongodb")
class MongoDBConnector:
    Config = MongoDBConfig

    def __init__(self, use_async: bool = True):
        self.use_async = use_async
        self._async_client = None
        self._sync_client = None
        self._config: MongoDBConfig | None = None

    async def connect(self, config: MongoDBConfig) -> None:
        logger.info("Connecting MongoDB: db=%s", config.database)
        try:
            self._config = config
            if self.use_async:
                self._async_client = motor.motor_asyncio.AsyncIOMotorClient(
                    config.connection_string,
                    maxPoolSize=config.max_pool_size,
                    minPoolSize=config.min_pool_size,
                )
                await self._async_client.admin.command("ping")
            else:
                self._sync_client = MongoClient(
                    config.connection_string,
                    maxPoolSize=config.max_pool_size,
                    minPoolSize=config.min_pool_size,
                )
                await asyncio.get_running_loop().run_in_executor(
                    None, self._sync_client.admin.command, "ping"
                )
            logger.info("Connected MongoDB: %s", config.database)
        except Exception as e:
            raise ConnConnectionError(
                f"Failed to connect to MongoDB: {e}", "mongodb"
            ) from e

    async def disconnect(self) -> None:
        logger.info("Disconnecting MongoDB")
        if self._async_client:
            self._async_client.close()
        if self._sync_client:
            await asyncio.get_running_loop().run_in_executor(None, self._sync_client.close)

    async def test_connection(self) -> bool:
        try:
            if self.use_async and self._async_client:
                await self._async_client.admin.command("ping")
            elif self._sync_client:
                await asyncio.get_running_loop().run_in_executor(
                    None, self._sync_client.admin.command, "ping"
                )
            return True
        except Exception:
            return False

    async def list_tables(self, config: MongoDBConfig) -> list[str]:
        db = self._get_database()
        if self.use_async:
            names = await db.list_collection_names()
        else:
            names = await asyncio.get_running_loop().run_in_executor(
                None, db.list_collection_names
            )
        return sorted(names)

    async def list_databases(self, config: MongoDBConfig) -> list[str]:
        if self.use_async:
            names = await self._async_client.list_database_names()
        else:
            names = await asyncio.get_running_loop().run_in_executor(
                None, self._sync_client.list_database_names
            )
        return sorted(names)

    async def get_schema(self, collection_name: str) -> pa.Schema:
        db = self._get_database()
        collection = db[collection_name]

        if self.use_async:
            docs = await collection.find().limit(1000).to_list(1000)
        else:
            docs = await asyncio.get_running_loop().run_in_executor(
                None, lambda: list(collection.find().limit(1000))
            )

        if not docs:
            return pa.schema([])

        df = pd.DataFrame([self._serialize_doc(d) for d in docs])
        return pa.Table.from_pandas(df).schema

    @staticmethod
    def _serialize_doc(doc: dict) -> dict:
        result = {}
        for key, val in doc.items():
            if val is None:
                result[key] = None
            elif isinstance(val, dict):
                result[key] = MongoDBConnector._serialize_doc(val)
            elif isinstance(val, list):
                result[key] = str([MongoDBConnector._serialize_doc(v) if isinstance(v, dict) else v for v in val])
            else:
                result[key] = str(val)
        return result

    def _get_database(self):
        db_name = self._config.database if self._config else None
        if self.use_async:
            return self._async_client.get_database(db_name)
        return self._sync_client.get_database(db_name)

    async def extract(
        self,
        collection_name: str,
        config: MongoDBConfig,
        filter_dict: dict | None = None,
        projection: dict | None = None,
    ) -> ExtractResult:
        logger.info("Extracting MongoDB collection: %s", collection_name)
        try:
            self._config = config
            db = self._get_database()
            collection = db[collection_name]

            effective_filter = dict(filter_dict or {})
            incremental_field = config.incremental_field
            sort_clause = []
            if incremental_field:
                sort_clause = [(incremental_field, 1)]
                if config.checkpoint_file:
                    cp = CheckpointFile(config.checkpoint_file)
                    checkpoint = cp.get(collection_name)
                    if checkpoint is not None:
                        effective_filter[incremental_field] = {"$gt": checkpoint}

            if self.use_async:
                cursor = collection.find(effective_filter, projection)
                if sort_clause:
                    cursor = cursor.sort(sort_clause)
                cursor = cursor.batch_size(config.batch_size)

                async def batch_generator() -> AsyncIterator[Batch]:
                    batch_num = 0
                    batch_docs = []
                    async for doc in cursor:
                        batch_docs.append(self._serialize_doc(doc))
                        if len(batch_docs) >= config.batch_size:
                            df = pd.DataFrame(batch_docs)
                            table = pa.Table.from_pandas(df)
                            logger.info("Batch %d: %d documents, %d bytes", batch_num, len(df), table.nbytes)
                            yield Batch(
                                data=table,
                                metadata=BatchMetadata(
                                    source_name="mongodb",
                                    table_name=collection_name,
                                    batch_id=f"{collection_name}_{batch_num}",
                                    row_count=len(df),
                                    byte_size=table.nbytes,
                                    schema=table.schema,
                                ),
                            )
                            batch_docs = []
                            batch_num += 1

                    if batch_docs:
                        df = pd.DataFrame(batch_docs)
                        table = pa.Table.from_pandas(df)
                        logger.info("Batch %d: %d documents, %d bytes", batch_num, len(df), table.nbytes)
                        yield Batch(
                            data=table,
                            metadata=BatchMetadata(
                                source_name="mongodb",
                                table_name=collection_name,
                                batch_id=f"{collection_name}_{batch_num}",
                                row_count=len(df),
                                byte_size=table.nbytes,
                                schema=table.schema,
                            ),
                        )

                return ExtractResult(batches=batch_generator())

            batches = await asyncio.get_running_loop().run_in_executor(
                None, self._extract_sync_impl, collection_name, config, effective_filter, projection
            )

            async def async_batch_generator() -> AsyncIterator[Batch]:
                for batch in batches:
                    yield batch

            return ExtractResult(batches=async_batch_generator())
        except Exception as e:
            raise ConnectorError(
                f"MongoDB extract failed for {collection_name}: {e}", "mongodb", retryable=True
            ) from e

    def _extract_sync_impl(
        self,
        collection_name: str,
        config: MongoDBConfig,
        filter_dict: dict | None,
        projection: dict | None,
    ) -> list[Batch]:
        db = self._get_database()
        collection = db[collection_name]
        incremental_field = config.incremental_field
        sort_clause = [(incremental_field, 1)] if incremental_field else []
        cursor = collection.find(filter_dict or {}, projection)
        if sort_clause:
            cursor = cursor.sort(sort_clause)
        cursor = cursor.batch_size(config.batch_size)

        batches: list[Batch] = []
        batch_num = 0
        batch_docs = []
        for doc in cursor:
            batch_docs.append(self._serialize_doc(doc))
            if len(batch_docs) >= config.batch_size:
                df = pd.DataFrame(batch_docs)
                table = pa.Table.from_pandas(df)
                batches.append(Batch(
                    data=table,
                    metadata=BatchMetadata(
                        source_name="mongodb",
                        table_name=collection_name,
                        batch_id=f"{collection_name}_{batch_num}",
                        row_count=len(df),
                        byte_size=table.nbytes,
                        schema=table.schema,
                    ),
                ))
                batch_docs = []
                batch_num += 1

        if batch_docs:
            df = pd.DataFrame(batch_docs)
            table = pa.Table.from_pandas(df)
            batches.append(Batch(
                data=table,
                metadata=BatchMetadata(
                    source_name="mongodb",
                    table_name=collection_name,
                    batch_id=f"{collection_name}_{batch_num}",
                    row_count=len(df),
                    byte_size=table.nbytes,
                    schema=table.schema,
                ),
            ))

        return batches

    async def get_checkpoint(self, collection_name: str) -> dict | None:
        return None

    def supports_incremental(self) -> bool:
        return True
