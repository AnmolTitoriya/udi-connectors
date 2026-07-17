from udi_connectors._registry import create_source, create_target, list_sources, list_targets, migrate_all

import udi_connectors.postgresql
import udi_connectors.mongodb
import udi_connectors.sql
import udi_connectors.s3
import udi_connectors.file_upload
import udi_connectors.athena

__all__ = [
    "create_source",
    "create_target",
    "list_sources",
    "list_targets",
    "migrate_all",
]
