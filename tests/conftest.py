import pytest


@pytest.fixture
def s3_bucket() -> str:
    return "test-bucket"


@pytest.fixture
def s3_region() -> str:
    return "us-east-1"
