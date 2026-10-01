"""Shared pytest fixtures for the dtq test suite.

Uses a REAL local Redis (never a fake). Each builder agent should run its
tests against a distinct DB index to avoid cross-talk when test runs overlap:
  A (packages): 1, B (api/cli): 2, C (worker): 3.
Set DTQ_TEST_REDIS_DB accordingly. Final verification runs everything
serially on db 0.
"""

import os

import pytest
import pytest_asyncio
from redis.asyncio import Redis

REDIS_URL = os.environ.get("DTQ_TEST_REDIS_URL", "redis://localhost:6379")
TEST_DB = int(os.environ.get("DTQ_TEST_REDIS_DB", "0"))


def make_redis(db: int = TEST_DB) -> Redis:
    return Redis.from_url(
        f"{REDIS_URL}/{db}", decode_responses=True, socket_timeout=5
    )


@pytest_asyncio.fixture
async def redis_client() -> Redis:  # type: ignore[misc]
    client = make_redis()
    await client.ping()
    await client.flushdb()
    yield client
    await client.flushdb()
    await client.aclose()


@pytest.fixture
def anyio_backend() -> str:
    return "asyncio"
