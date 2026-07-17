from typing import Literal
from urllib.parse import quote_plus

from udi_packages.config import BaseConfig
from pydantic import SecretStr


class PostgreSQLConfig(BaseConfig):
    source_type: Literal["postgresql"] = "postgresql"

    host: str = "localhost"
    port: int = 5432
    database: str
    username: str
    password: SecretStr

    ssl_mode: Literal["disable", "allow", "prefer", "require", "verify-ca", "verify-full"] = "prefer"
    ssl_cert: str | None = None
    ssl_key: str | None = None
    ssl_root_cert: str | None = None

    pool_min_size: int = 2
    pool_max_size: int = 10
    pool_timeout: float = 30.0

    batch_size: int = 20_000
    cursor_name: str | None = None

    incremental_column: str | None = None
    last_checkpoint: dict | None = None
    checkpoint_file: str | None = None

    @property
    def connection_string(self) -> str:
        pwd = quote_plus(self.password.get_secret_value())
        params = []
        if self.ssl_mode != "prefer":
            params.append(f"sslmode={self.ssl_mode}")
        if self.ssl_cert:
            params.append(f"sslcert={self.ssl_cert}")
        if self.ssl_key:
            params.append(f"sslkey={self.ssl_key}")
        if self.ssl_root_cert:
            params.append(f"sslrootcert={self.ssl_root_cert}")
        param_str = f"?{'&'.join(params)}" if params else ""
        return f"postgresql://{self.username}:{pwd}@{self.host}:{self.port}/{self.database}{param_str}"
