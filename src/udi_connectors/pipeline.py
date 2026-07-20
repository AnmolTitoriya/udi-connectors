"""Stage 2 (transform) and Stage 3 (publish) of the raw -> curated pipeline.

Stage 1 (source -> S3 raw landing, via migrate_all in _registry.py) is
untouched by this module.

Layout convention used here, all under the S3 target's `curated` zone:

    curated/{connection_id}/{table_name}__staging/...   <- Stage 2 writes here
    curated/{connection_id}/{table_name}/...            <- Stage 3 publishes here

`__staging` is a *sibling* table name, not a subfolder of the published
table — s3 prefix matching is a plain string prefix, so
".../{table_name}/" and ".../{table_name}__staging/" must not nest inside
one another, or dataset_exists()/get_schema()/upsert on the published table
would pick up not-yet-published staging files.

Stage 2 always lands its transformed output as this isolated increment
rather than the published location directly, so Stage 3 has a well-defined
"new data" set to check for existence/schema drift and to append or upsert
against the previously published rows — without Stage 3 re-reading (and
re-deduping against) its own prior output.
"""

import logging
from collections.abc import AsyncIterator
from dataclasses import dataclass, field, replace
from typing import Literal

import pyarrow as pa
import pyarrow.compute as pc
from udi_packages import (
    Batch,
    CheckpointFile,
    ConnectorError,
    LoadResult,
    effective_batch_size,
    rechunk,
)

from ._registry import create_source, create_target

logger = logging.getLogger(__name__)

_STAGING_SUFFIX = "__staging"

_ARROW_TYPE_MAP: dict[str, pa.DataType] = {
    "string": pa.string(),
    "int32": pa.int32(),
    "int64": pa.int64(),
    "float32": pa.float32(),
    "float64": pa.float64(),
    "bool": pa.bool_(),
    "date": pa.date32(),
    "timestamp": pa.timestamp("us"),
}


# ---------------------------------------------------------------------------
# Transform hooks
# ---------------------------------------------------------------------------

@dataclass
class SqlTransform:
    """Manual transform: an arbitrary SQL statement run against Athena.

    Requires source_type="athena" — the direct S3 reader has no SQL engine
    behind it, so this can't be satisfied by the fallback reader.
    """
    sql: str


@dataclass
class RuleTransform:
    """Automatic transform: a declarative set of pyarrow-level operations,
    applied per-batch so it works against either reader (Athena or S3)."""
    rename: dict[str, str] = field(default_factory=dict)
    cast: dict[str, str] = field(default_factory=dict)
    drop_columns: list[str] = field(default_factory=list)
    drop_nulls: list[str] = field(default_factory=list)
    dedupe_keys: list[str] | None = None


Transform = SqlTransform | RuleTransform | None


def apply_rule_transform(table: pa.Table, rule: RuleTransform) -> pa.Table:
    if rule.rename:
        new_names = [rule.rename.get(name, name) for name in table.column_names]
        table = table.rename_columns(new_names)

    for column, type_name in rule.cast.items():
        if column not in table.column_names:
            continue
        arrow_type = _ARROW_TYPE_MAP.get(type_name)
        if arrow_type is None:
            logger.warning("Unknown cast type '%s' for column '%s' — skipped", type_name, column)
            continue
        idx = table.schema.get_field_index(column)
        table = table.set_column(idx, column, pc.cast(table.column(column), arrow_type))

    for column in rule.drop_nulls:
        if column in table.column_names:
            table = table.filter(pc.is_valid(table.column(column)))

    if rule.drop_columns:
        keep = [c for c in table.column_names if c not in rule.drop_columns]
        table = table.select(keep)

    if rule.dedupe_keys:
        table = _dedupe(table, rule.dedupe_keys)

    return table


def _dedupe(table: pa.Table, keys: list[str]) -> pa.Table:
    if table.num_rows == 0:
        return table
    df = table.to_pandas()
    df = df.drop_duplicates(subset=keys, keep="last")
    return pa.Table.from_pandas(df, preserve_index=False)


async def _replay(batches: list[Batch]) -> AsyncIterator[Batch]:
    for b in batches:
        yield b


# ---------------------------------------------------------------------------
# Stage 2 — transform: raw -> curated/_staging
# ---------------------------------------------------------------------------

async def migrate_raw_to_curated(
    connection_id: str,
    table_name: str,
    transform: Transform = None,
    source_type: Literal["athena", "s3"] = "athena",
    source_kwargs: dict | None = None,
    target_kwargs: dict | None = None,
    batch_size: int | None = None,
) -> LoadResult:
    """Read the raw zone (Athena SQL, or the direct S3 reader as a fallback),
    apply `transform`, and land the result in curated/.../_staging/, rechunked
    to whatever the curated S3 target can accept."""
    source_kwargs = dict(source_kwargs or {})
    target_kwargs = dict(target_kwargs or {})

    if isinstance(transform, SqlTransform) and source_type != "athena":
        raise ConnectorError(
            "SQL transforms require the Athena source — the direct S3 reader can't run arbitrary SQL",
            "pipeline",
            retryable=False,
        )

    if source_type == "s3":
        source_kwargs.setdefault("connection_id", connection_id)
        source_kwargs.setdefault("zone", "raw")

    target_kwargs.setdefault("connection_id", connection_id)
    target_kwargs["zone"] = "curated"

    src, src_cfg = create_source(source_type, **source_kwargs)
    tgt, tgt_cfg = create_target("s3", **target_kwargs)

    staging_table = f"{table_name}{_STAGING_SUFFIX}"
    logger.info(
        "Stage 2 transform started: connection=%s table=%s source=%s transform=%s",
        connection_id, table_name, source_type, type(transform).__name__ if transform else None,
    )

    await src.connect(src_cfg)
    await tgt.connect(tgt_cfg)

    checkpoint_file = getattr(src_cfg, "checkpoint_file", None)
    cp = CheckpointFile(checkpoint_file) if checkpoint_file else None

    try:
        if isinstance(transform, SqlTransform):
            result = await src.extract_sql(transform.sql, table_name)
        else:
            result = await src.extract(table_name, src_cfg)

        async def transformed() -> AsyncIterator[Batch]:
            async for batch in result.batches:
                data = batch.data
                if isinstance(transform, RuleTransform):
                    data = apply_rule_transform(data, transform)
                metadata = replace(batch.metadata, zone="curated", table_name=table_name, schema=data.schema, row_count=data.num_rows, byte_size=data.nbytes)
                yield Batch(data=data, metadata=metadata)

        capabilities = tgt.get_capabilities()
        source_batch_size = batch_size or getattr(src_cfg, "batch_size", capabilities.max_batch_size)
        resolved_size = effective_batch_size(source_batch_size, capabilities)

        async def checkpointed() -> AsyncIterator[Batch]:
            # Checkpointing happens per resized (post-rechunk) batch, not per
            # original source batch — the source batch boundary is meaningless
            # once batches have been split/coalesced for the target.
            async for resized in rechunk(transformed(), resolved_size):
                if cp:
                    cp.set(resized.metadata.batch_id, {
                        "table": table_name,
                        "row_count": resized.metadata.row_count,
                        "extracted_at": resized.metadata.extracted_at.isoformat(),
                    })
                yield resized

        load_result = await tgt.load(checkpointed(), staging_table, mode="append")
    finally:
        await src.disconnect()
        await tgt.disconnect()

    logger.info(
        "Stage 2 transform complete: connection=%s table=%s rows=%d batches=%d errors=%d",
        connection_id, table_name, load_result.rows_loaded, load_result.batch_count, len(load_result.errors),
    )
    return load_result


# ---------------------------------------------------------------------------
# Stage 3 — publish: curated/_staging -> curated/ (create / append / upsert)
# ---------------------------------------------------------------------------

@dataclass
class PublishResult:
    table_name: str
    action: Literal["create", "append", "upsert"]
    schema_changed: bool
    rows_loaded: int
    batch_count: int
    errors: list[str]


async def publish_curated(
    connection_id: str,
    table_name: str,
    merge_keys: list[str] | None = None,
    target_kwargs: dict | None = None,
) -> PublishResult:
    """Move Stage 2's staged increment into the published curated dataset.

    Runs exactly one dataset_exists()/get_schema() check up front, then
    branches once for the whole run — not per batch:
      - no existing data            -> create (plain write)
      - existing data, no merge key -> append (plain write)
      - existing data + merge key   -> upsert (delete-then-rewrite by key)
    """
    target_kwargs = dict(target_kwargs or {})
    target_kwargs.setdefault("connection_id", connection_id)
    target_kwargs["zone"] = "curated"

    tgt, tgt_cfg = create_target("s3", **target_kwargs)
    await tgt.connect(tgt_cfg)

    staging_table = f"{table_name}{_STAGING_SUFFIX}"

    try:
        existed_before = await tgt.dataset_exists(table_name)
        previous_schema = await tgt.get_schema(table_name) if existed_before else None

        staging_result = await tgt.extract(staging_table, tgt_cfg)
        staging_batches = [b async for b in staging_result.batches]

        if not staging_batches:
            logger.info(
                "Stage 3 publish: nothing staged for connection=%s table=%s — nothing to do",
                connection_id, table_name,
            )
            return PublishResult(
                table_name=table_name,
                action="append" if existed_before else "create",
                schema_changed=False,
                rows_loaded=0,
                batch_count=0,
                errors=[],
            )

        new_schema = staging_batches[0].metadata.schema
        schema_changed = bool(
            existed_before and previous_schema is not None
            and set(previous_schema.names) != set(new_schema.names)
        )
        if schema_changed:
            logger.warning(
                "Curated schema drift for connection=%s table=%s: %s -> %s. "
                "This pipeline does not auto-update the Glue table definition — "
                "re-run the crawler (or issue an ALTER TABLE) before querying via Athena.",
                connection_id, table_name, previous_schema.names, new_schema.names,
            )

        if not existed_before:
            action: Literal["create", "append", "upsert"] = "create"
        elif merge_keys:
            action = "upsert"
        else:
            action = "append"

        mode: Literal["append", "upsert"] = "upsert" if action == "upsert" else "append"
        load_result = await tgt.load(_replay(staging_batches), table_name, mode=mode, merge_keys=merge_keys)

        await tgt.clear(staging_table)
    finally:
        await tgt.disconnect()

    logger.info(
        "Stage 3 publish complete: connection=%s table=%s action=%s rows=%d batches=%d errors=%d",
        connection_id, table_name, action, load_result.rows_loaded, load_result.batch_count, len(load_result.errors),
    )
    return PublishResult(
        table_name=table_name,
        action=action,
        schema_changed=schema_changed,
        rows_loaded=load_result.rows_loaded,
        batch_count=load_result.batch_count,
        errors=load_result.errors,
    )
