import asyncio
import datetime
import logging
from collections.abc import AsyncIterator
from pathlib import Path

from udi_packages import Batch, CheckpointFile, ConnectorError, LoadResult

_sources: dict[str, type] = {}
_targets: dict[str, type] = {}

logger = logging.getLogger(__name__)


class Source:
    def __init__(self, name: str):
        self._name = name

    def __call__(self, cls):
        _sources[self._name] = cls
        return cls


class Target:
    def __init__(self, name: str):
        self._name = name

    def __call__(self, cls):
        _targets[self._name] = cls
        return cls


def register_source(name: str, connector_cls: type) -> None:
    """Function-form equivalent of @Source(name) — for connectors that
    register themselves programmatically (e.g. a plugin loaded at runtime)
    rather than via the decorator at import time."""
    _sources[name] = connector_cls


def register_target(name: str, connector_cls: type) -> None:
    """Function-form equivalent of @Target(name)."""
    _targets[name] = connector_cls


def get_source_class(name: str) -> type | None:
    """The raw connector class for a registered source, or None. Custom and
    built-in connectors are indistinguishable here — both just live in the
    same _sources dict, keyed by whatever name they registered under."""
    return _sources.get(name)


def get_target_class(name: str) -> type | None:
    return _targets.get(name)


def load_plugins(group: str = "udi_connectors.plugins") -> list[str]:
    """Import every module registered under the `udi_connectors.plugins`
    entry-point group, so a pip-installed third-party package can add a
    custom connector with zero changes to this repo. Each entry point's
    target module is expected to self-register on import via @Source(...)/
    @Target(...) (or register_source/register_target) — the same mechanism
    the built-in connectors use. A broken plugin is logged and skipped
    rather than taking down the whole registry.
    """
    import importlib
    from importlib.metadata import entry_points

    loaded: list[str] = []
    try:
        eps = entry_points(group=group)
    except Exception as e:
        logger.warning("Could not enumerate connector plugins: %s", e)
        return loaded

    for ep in eps:
        try:
            importlib.import_module(ep.value)
            loaded.append(ep.name)
            logger.info("Loaded connector plugin: %s (%s)", ep.name, ep.value)
        except Exception as e:
            logger.warning("Failed to load connector plugin '%s' (%s): %s", ep.name, ep.value, e)
    return loaded


def load_plugin_dir(path: str | None = None) -> list[str]:
    """Import every top-level .py file in `path` (default:
    $UDI_CONNECTOR_PLUGINS_DIR) so it can self-register, the same way
    load_plugins() does for packaged plugins — a lower-ceremony option for a
    custom connector that isn't (yet) worth publishing as its own package.
    """
    import importlib.util
    import os as _os

    path = path or _os.environ.get("UDI_CONNECTOR_PLUGINS_DIR")
    if not path:
        return []
    plugin_dir = Path(path)
    if not plugin_dir.is_dir():
        logger.warning("UDI_CONNECTOR_PLUGINS_DIR=%s is not a directory — skipping", path)
        return []

    loaded: list[str] = []
    for f in sorted(plugin_dir.glob("*.py")):
        if f.stem.startswith("_"):
            continue
        try:
            spec = importlib.util.spec_from_file_location(f"udi_connectors._plugin_{f.stem}", f)
            if spec is None or spec.loader is None:
                continue
            module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(module)
            loaded.append(f.stem)
            logger.info("Loaded connector plugin from file: %s", f)
        except Exception as e:
            logger.warning("Failed to load connector plugin file '%s': %s", f, e)
    return loaded


def create_source(name: str, **config_kwargs) -> tuple:
    if name not in _sources:
        raise ValueError(f"Unknown source: {name}. Available: {list(_sources.keys())}")
    connector_cls = _sources[name]
    config_cls = connector_cls.Config
    config = config_cls(**config_kwargs)
    connector = connector_cls()
    return connector, config


def create_target(name: str, **config_kwargs) -> tuple:
    if name not in _targets:
        raise ValueError(f"Unknown target: {name}. Available: {list(_targets.keys())}")
    connector_cls = _targets[name]
    config_cls = connector_cls.Config
    config = config_cls(**config_kwargs)
    connector = connector_cls()
    return connector, config


def list_sources() -> list[str]:
    return list(_sources.keys())


def list_targets() -> list[str]:
    return list(_targets.keys())


async def migrate_all(
    source_name: str,
    target_name: str,
    tables: list[str] | None = None,
    source_kwargs: dict | None = None,
    target_kwargs: dict | None = None,
    s3_folder: str | None = None,
) -> list:
    source_kwargs = source_kwargs or {}
    target_kwargs = target_kwargs or {}

    logger.info("Migration started: %s -> %s, tables=%s", source_name, target_name, tables)

    src, src_cfg = create_source(source_name, **source_kwargs)
    tgt, tgt_cfg = create_target(target_name, **target_kwargs)

    results: list = []
    try:
        logger.info("Connecting source=%s target=%s", source_name, target_name)
        await src.connect(src_cfg)
        await tgt.connect(tgt_cfg)
        logger.info("Connected source=%s target=%s", source_name, target_name)

        table_list = tables if tables else [src_cfg.database if hasattr(src_cfg, "database") else "data"]

        for table_name in table_list:
            last_error = None

            folder = s3_folder or table_name

            for attempt in range(3):
                try:
                    logger.info("Extracting table=%s from source=%s (attempt %d/3)", table_name, source_name, attempt + 1)
                    result = await src.extract(table_name, src_cfg)

                    total_rows = 0
                    total_batches = 0
                    batch_errors: list[str] = []

                    async for batch in result.batches:
                        async def single_gen(b=batch) -> AsyncIterator[Batch]:
                            yield b

                        load_result = await tgt.load(single_gen(), folder)

                        if load_result.errors:
                            raise ConnectorError(
                                f"Load failed for {table_name} batch {batch.metadata.batch_id}: {'; '.join(load_result.errors)}",
                                source_name,
                                retryable=True,
                            )

                        _save_checkpoint(src_cfg, table_name, batch)
                        total_rows += load_result.rows_loaded
                        total_batches += load_result.batch_count
                        logger.info("Loaded batch %s: %d rows", batch.metadata.batch_id, batch.metadata.row_count)

                    results.append(LoadResult(
                        destination_type=target_name,
                        table_name=table_name,
                        rows_loaded=total_rows,
                        batch_count=total_batches,
                        errors=batch_errors,
                    ))
                    logger.info("Completed table=%s: rows=%d batches=%d", table_name, total_rows, total_batches)
                    last_error = None
                    break
                except Exception as e:
                    last_error = e
                    logger.warning("Attempt %d/3 failed for table %s: %s", attempt + 1, table_name, e)
                    if attempt < 2:
                        await asyncio.sleep(2 ** attempt)

            if last_error is not None:
                raise last_error

    finally:
        logger.info("Disconnecting source=%s target=%s", source_name, target_name)
        await src.disconnect()
        await tgt.disconnect()

    logger.info("Migration finished: %d tables processed", len(results))
    return results


def _save_checkpoint(config, table_name: str, batch: Batch) -> None:
    cp_file = getattr(config, "checkpoint_file", None)
    if not cp_file:
        return

    cp = CheckpointFile(cp_file)
    incremental_col = getattr(config, "incremental_column", None) or getattr(config, "incremental_field", None)

    if incremental_col:
        col_idx = batch.data.schema.get_field_index(incremental_col)
        if col_idx is not None and col_idx >= 0:
            col = batch.data.column(col_idx)
            non_null = [v.as_py() for v in col if v.as_py() is not None]
            if non_null:
                cp.set(table_name, non_null[-1])
    else:
        filename_idx = batch.data.schema.get_field_index("filename")
        if filename_idx is not None and filename_idx >= 0:
            filenames = [v.as_py() for v in batch.data.column(filename_idx) if v.as_py() is not None]
            if filenames:
                existing = cp.get(table_name) or []
                if isinstance(existing, list):
                    existing.extend(filenames)
                else:
                    existing = filenames
                cp.set(table_name, existing)
