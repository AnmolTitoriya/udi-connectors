from typing import Literal

from udi_packages.config import BaseConfig


class AthenaConfig(BaseConfig):
    source_type: Literal["athena"] = "athena"

    region: str = "us-east-1"
    catalog: str = "AwsDataCatalog"
    database: str
    workgroup: str = "primary"
    output_location: str | None = None

    access_key: str | None = None
    secret_key: str | None = None
    session_token: str | None = None

    batch_size: int = 1_000
    poll_interval: float = 1.0
    max_poll_attempts: int = 300

    incremental_column: str | None = None
    checkpoint_file: str | None = None
