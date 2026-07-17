import asyncio
import platform

import pytest


@pytest.fixture(scope="session")
def event_loop_policy():
    if platform.system() == "Windows":
        return asyncio.WindowsSelectorEventLoopPolicy()
    return asyncio.DefaultEventLoopPolicy()
