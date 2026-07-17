from typing import Any, Literal

from udi_packages.config import BaseConfig
from pydantic import Field, SecretStr


class SQLConfig(BaseConfig):
    source_type: Literal["sql"] = "sql"
    dialect: Literal["postgresql", "mysql", "mssql", "oracle", "sqlite"] = "postgresql"

    host: str = "localhost"
    port: int = 5432
    database: str
    username: str
    password: SecretStr

    driver: str | None = None
    extra_params: dict[str, Any] = Field(default_factory=dict)

    pool_size: int = 10
    max_overflow: int = 20
    pool_timeout: float = 30.0

    batch_size: int = 20_000
    incremental_column: str | None = None
    last_checkpoint: dict | None = None
    checkpoint_file: str | None = None
