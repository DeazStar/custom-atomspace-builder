"""Tests for the Redis-backed UploadStateStore state machine."""

import json
from datetime import datetime, timedelta, timezone

import pytest

from app.core.upload_state_store import UploadStateStore


@pytest.fixture
def store(session_id):
    """A store instance bound to the isolated test Redis DB."""
    from app.core.upload_state_store import upload_state_store
    return upload_state_store


def test_create_and_get_session(store, session_id):
    session = store.get_session(session_id)
    assert session is not None
    assert session["session_id"] == session_id
    assert session["status"] == "active"
    assert session["uploaded_files"] == []


def test_expired_session_reported_missing(store):
    now = datetime.now(tz=timezone.utc)
    sid = "expired-session"
    store.create_session(sid, created_at=now - timedelta(hours=2),
                         expires_at=now - timedelta(hours=1))
    assert store.get_session(sid) is None


def test_add_file_duplicate_detection_is_atomic(store, session_id):
    first = store.add_file(session_id, "a.csv", 10, "2026-01-01T00:00:00+00:00")
    assert first is not None
    # Second registration of the same name must be rejected atomically
    assert store.add_file(session_id, "a.csv", 10, "2026-01-01T00:00:00+00:00") is None
    # The generation token must be stable across status changes
    assert store.get_upload_id(session_id, "a.csv") == first
    files = store.get_files(session_id)
    assert len(files) == 1


def test_each_upload_gets_a_fresh_generation_token(store, session_id):
    first = store.add_file(session_id, "a.csv", 10, "2026-01-01T00:00:00+00:00")
    store.mark_deleted(session_id, "a.csv")
    second = store.add_file(session_id, "a.csv", 20, "2026-01-01T00:00:00+00:00")
    assert first is not None and second is not None
    assert first != second
    assert store.get_upload_id(session_id, "a.csv") == second


def test_status_transitions(store, session_id):
    store.add_file(session_id, "a.csv", 10, "2026-01-01T00:00:00+00:00", status="queued")
    assert store.get_file_status(session_id, "a.csv") == "queued"

    store.set_file_status(session_id, "a.csv", "processing")
    assert store.get_file_status(session_id, "a.csv") == "processing"

    store.set_file_status(session_id, "a.csv", "processed")
    assert store.get_file_status(session_id, "a.csv") == "processed"
    meta = store.get_file(session_id, "a.csv")
    assert meta["filename"] == "a.csv"
    assert meta["size"] == 10


def test_error_field_roundtrip(store, session_id):
    store.add_file(session_id, "bad.csv", 0, "2026-01-01T00:00:00+00:00")
    store.set_file_status(session_id, "bad.csv", "failed", error="boom")
    meta = store.get_file(session_id, "bad.csv")
    assert meta["status"] == "failed"
    assert meta["error"] == "boom"


def test_mark_deleted_tombstone_and_readd(store, session_id):
    store.add_file(session_id, "a.csv", 10, "2026-01-01T00:00:00+00:00")
    store.mark_deleted(session_id, "a.csv")
    assert store.is_deleted(session_id, "a.csv") is True
    assert store.get_file(session_id, "a.csv") is None
    assert store.get_files(session_id) == []

    # Re-uploading the same name after deletion must be allowed (new generation)
    assert store.add_file(session_id, "a.csv", 20, "2026-01-01T00:00:00+00:00") is not None
    assert store.is_deleted(session_id, "a.csv") is False


def test_file_order_is_preserved(store, session_id):
    for i, name in enumerate(["b.csv", "a.csv", "c.csv"]):
        store.add_file(session_id, name, i, "2026-01-01T00:00:00+00:00")
    assert [f["filename"] for f in store.get_files(session_id)] == ["b.csv", "a.csv", "c.csv"]


def test_cleanup_expired(store):
    now = datetime.now(tz=timezone.utc)
    sid = "will-expire"
    store.create_session(sid, created_at=now - timedelta(hours=2),
                         expires_at=now - timedelta(hours=1))
    store.add_file(sid, "a.csv", 1, "2026-01-01T00:00:00+00:00")
    expired = store.cleanup_expired()
    assert sid in expired
    assert store.get_session(sid) is None
