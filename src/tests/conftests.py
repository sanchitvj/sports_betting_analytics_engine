import pytest
import asyncio
import logging


@pytest.fixture(scope="session")
def event_loop():
    """Create an instance of the default event loop for each test case."""
    policy = asyncio.get_event_loop_policy()
    loop = policy.new_event_loop()
    yield loop
    loop.close()


@pytest.fixture
def caplog(caplog):
    """Provide access to captured logs."""
    caplog.set_level(logging.INFO)
    return caplog
