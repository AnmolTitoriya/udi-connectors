from typing import Literal

from udi_packages.config import BaseConfig


class FileUploadConfig(BaseConfig):
    source_type: Literal["file_upload"] = "file_upload"

    input_dir: str
    file_pattern: str = "*"
    files: list[str] | None = None
    recursive: bool = False
    batch_size: int = 100
    include_content: bool = True
    checkpoint_file: str | None = None
