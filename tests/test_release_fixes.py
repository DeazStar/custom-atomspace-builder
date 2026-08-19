"""Regression tests for the release-readiness fixes:

C1 - a multi-file upload where one file fails to persist must roll back every
     file registered by that request (never leaving earlier files stuck in
     'queued' with no Celery task), and must never delete files that existed
     before the request.
M1 - a Celery task that cannot persist its terminal status (e.g. Redis down)
     must re-raise instead of being acknowledged as a success.
"""

import asyncio
import os

import pytest
from fastapi import HTTPException

from app.api import upload as upload_mod
from app.core.session_manager import session_manager
from app.core.upload_state_store import upload_state_store, StateStoreUnavailable
from app.tasks.upload_tasks import process_uploaded_files_task


def _write_file(session_id, filename, content):
    session_dir = session_manager._get_session_dir(session_id)
    os.makedirs(session_dir, exist_ok=True)
    path = os.path.join(session_dir, filename)
    with open(path, "w", encoding="utf-8") as fh:
        fh.write(content)
    return path


def _register(session_id, filename, content):
    path = _write_file(session_id, filename, content)
    upload_id = session_manager.add_file_to_session(session_id, filename)
    assert upload_id is not None
    upload_state_store.update_file_metadata(session_id, filename,
                                            os.path.getsize(path),
                                            "2026-01-01T00:00:00+00:00")
    return upload_id


class _FakeUploadFile:
    """Minimal UploadFile stand-in used by the upload endpoint: it only ever
    accesses ``.filename`` and ``await .read(size)``."""

    def __init__(self, filename, data=b"", fail_read=False):
        self.filename = filename
        self._data = data
        self._fail_read = fail_read

    async def read(self, size=-1):
        if self._fail_read:
            raise OSError("simulated disk write failure")
        chunk, self._data = self._data[:size], self._data[size:]
        return chunk


# ---- C1: multi-file persist failure must not orphan earlier files ----------

def test_multi_file_persist_failure_rolls_back_all_request_files(session_id, monkeypatch):
    """A file persisted earlier in the same request must be rolled back when a
    later file fails to persist; nothing is enqueued; files that existed before
    the request survive."""
    keep_path = _write_file(session_id, "keep.csv", "id,name\n0,pre\n")
    keep_id = session_manager.add_file_to_session(session_id, "keep.csv")
    assert keep_id is not None
    upload_state_store.update_file_metadata(session_id, "keep.csv",
                                            os.path.getsize(keep_path),
                                            "2026-01-01T00:00:00+00:00")

    def _must_not_enqueue(*args, **kwargs):
        raise AssertionError("must not enqueue after a failed multi-file upload")

    monkeypatch.setattr(upload_mod.process_uploaded_files_task, "delay",
                        _must_not_enqueue)

    files = [
        _FakeUploadFile("drop1.csv", b"id,name\n1,x\n"),
        _FakeUploadFile("drop2.csv", b"id,name\n2,y\n", fail_read=True),
    ]

    with pytest.raises(HTTPException) as excinfo:
        asyncio.run(upload_mod.upload_files(session_id, files))
    assert excinfo.value.status_code == 500
    assert "drop2.csv" in excinfo.value.detail

    # drop1 (persisted OK earlier in the request) and drop2 are fully rolled
    # back: no Redis state, tombstoned, physical files removed, out of session.
    session = session_manager.get_session(session_id)
    session_dir = session_manager._get_session_dir(session_id)
    for name in ("drop1.csv", "drop2.csv"):
        assert upload_state_store.get_file(session_id, name) is None
        assert upload_state_store.is_deleted(session_id, name) is True
        assert not os.path.exists(os.path.join(session_dir, name))
        assert name not in session.uploaded_files

    # The file that existed before the request is untouched.
    assert "keep.csv" in session.uploaded_files
    assert os.path.exists(keep_path)
    assert upload_state_store.get_file(session_id, "keep.csv") is not None
    assert upload_state_store.is_deleted(session_id, "keep.csv") is False


def test_duplicate_mid_request_rolls_back_earlier_files_not_pre_existing(session_id, monkeypatch):
    """A duplicate name aborts the request (400); files registered earlier in
    the same request are rolled back, while the pre-existing duplicate file is
    not deleted."""
    _register(session_id, "dup.csv", "id,name\n0,pre\n")

    def _must_not_enqueue(*args, **kwargs):
        raise AssertionError("must not enqueue after a failed multi-file upload")

    monkeypatch.setattr(upload_mod.process_uploaded_files_task, "delay",
                        _must_not_enqueue)

    files = [
        _FakeUploadFile("new1.csv", b"id,name\n1,x\n"),
        _FakeUploadFile("dup.csv", b"id,name\n2,y\n"),
    ]

    with pytest.raises(HTTPException) as excinfo:
        asyncio.run(upload_mod.upload_files(session_id, files))
    assert excinfo.value.status_code == 400

    session = session_manager.get_session(session_id)
    # new1 (registered by this request) is rolled back...
    assert upload_state_store.get_file(session_id, "new1.csv") is None
    assert not os.path.exists(os.path.join(
        session_manager._get_session_dir(session_id), "new1.csv"))
    assert "new1.csv" not in session.uploaded_files
    # ...but the pre-existing duplicate is untouched.
    assert upload_state_store.get_file(session_id, "dup.csv") is not None
    assert "dup.csv" in session.uploaded_files
    assert upload_state_store.get_upload_id(session_id, "dup.csv") is not None


# ---- M1: Redis state-store failure must not ack a task as success -----------

def test_task_reraises_when_terminal_status_cannot_be_persisted(session_id, monkeypatch):
    """When Redis goes away at the terminal status write, the task must re-raise
    StateStoreUnavailable so Celery redelivers it (acks_late) instead of acking
    a task whose terminal state was never recorded."""
    upload_id = _register(session_id, "a.csv", "id,name\n1,x\n")

    def flaky(sid, filename, status, error=None):
        if status == "processing":
            return True  # allow the file to start processing
        raise StateStoreUnavailable("redis down")

    monkeypatch.setattr(upload_state_store, "set_file_status", flaky)

    with pytest.raises(StateStoreUnavailable):
        process_uploaded_files_task(
            session_id, [{"filename": "a.csv", "upload_id": upload_id}])


def test_task_records_failed_and_returns_when_store_available(session_id):
    """Control: with the state store healthy, a genuinely failing file is
    recorded as 'failed' and the task returns normally (no raise)."""
    upload_id = _register(session_id, "empty.csv", "")
    result = process_uploaded_files_task(
        session_id, [{"filename": "empty.csv", "upload_id": upload_id}])
    assert result["files"] == {"empty.csv": "failed"}
    assert upload_state_store.get_file_status(session_id, "empty.csv") == "failed"