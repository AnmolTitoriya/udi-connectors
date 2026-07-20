from typing import Literal

from udi_packages.config import BaseConfig


class S3Config(BaseConfig):
    target_type: Literal["s3"] = "s3"
    source_type: Literal["s3"] = "s3"

    bucket_name: str
    prefix: str = ""
    region: str = "us-east-1"
    endpoint_url: str | None = None
    access_key: str | None = None
    secret_key: str | None = None
    session_token: str | None = None

    file_format: Literal["parquet", "csv", "jsonl"] = "parquet"
    compression: Literal["snappy", "gzip", "none"] = "snappy"

    batch_size: int = 100_000
    max_concurrent_uploads: int = 5
    retry_count: int = 3
    retry_delay: float = 1.0

    # Landing-zone partitioning: keys are laid out as
    #   {prefix}/{zone}/{connection_id}/{table_name}/dt={extracted_at:%Y-%m-%d}/{batch_id}.ext
    # `zone` is the default used when a batch doesn't already carry one via
    # BatchMetadata.zone (e.g. batches produced outside the pipeline helpers).
    connection_id: str | None = None
    zone: Literal["raw", "curated"] = "raw"

    incremental_column: str | None = None
    checkpoint_file: str | None = None
