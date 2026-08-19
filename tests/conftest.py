"""Shared pytest fixtures for the upload/preprocessing tests.

All tests run against a dedicated Redis database (db 15) so they never touch
developer data, and against a temp output dir so they never write into the
repo's real ``output/`` tree. They are skipped when Redis is unavailable.
"""

import os
import sys
import tempfile

import pytest
import redis

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, REPO_ROOT)

from app.config import settings  # noqa: E402
from app.core.upload_state_store import upload_state_store  # noqa: E402

TEST_REDIS_URL = "redis://localhost:6379/15"


@pytest.fixture(scope="session", autouse=True)
def _redis_available():
    try:
        client = redis.Redis.from_url(TEST_REDIS_URL, socket_connect_timeout=1,
                                      decode_responses=True)
        client.ping()
    except Exception as exc:  # noqa: BLE001
        pytest.skip(f"Redis unavailable: {exc}")
    return client


@pytest.fixture(autouse=True)
def _isolated_env(_redis_available, tmp_path, monkeypatch):
    """Point the shared state store at the test DB and flush it, and redirect
    the output dir to a temp location."""
    test_client = _redis_available
    test_client.flushdb()

    monkeypatch.setattr(upload_state_store, "_redis_url", TEST_REDIS_URL)
    monkeypatch.setattr(upload_state_store, "_redis", test_client)

    # Redirect where session dirs are created (shared settings singleton)
    output_dir = str(tmp_path / "output")
    os.makedirs(output_dir, exist_ok=True)
    monkeypatch.setattr(settings, "base_output_dir", output_dir)

    yield

    test_client.flushdb()


@pytest.fixture
def session_id(_isolated_env):
    from app.core.session_manager import session_manager
    return session_manager.create_session()
