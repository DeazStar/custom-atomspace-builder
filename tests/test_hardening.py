"""Hardening tests for the review findings:

- generation-based file identity (stale Celery tasks cannot clobber a
  re-uploaded file with the same name)
- cache write failures are terminal, not false "processed"
- delete vs. in-flight processing race (tombstone + cache removal share one
  lock; a worker must not resurrect the cache entry)
- Redis TTLs are applied when per-file keys are created (not only at
  create_session, when the keys do not exist yet)
- persist-failure rollback removes the registration and partial file
"""

import os
import threading

import pytest

from app.core.session_manager import session_manager
from app.core.upload_state_store import upload_state_store
from app.services.file_parser_service import file_parser_service
from app.models.schemas import DataSource, FileInfo


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


def _ds(filename):
    return DataSource(id=f"ds_{filename}", file=FileInfo(name=filename, size=1, type="text/csv"),
                      columns=["id", "name"], sampleRow=["1", "alice"])


# ---- generation-based identity ----------------------------------------

def test_stale_generation_task_cannot_write_new_file(session_id):
    """Delete + re-upload the same name: a task carrying the OLD generation
    token must not write cache/status for the new file; the NEW file's task
    still processes it normally."""
    old_id = _register(session_id, "a.csv", "id,name\n1,old\n")
    session_manager.remove_file_from_session(session_id, "a.csv")
    new_id = _register(session_id, "a.csv", "id,name\n2,new\n")
    assert new_id != old_id

    # Stale task runs (as if it had been queued before the re-upload)
    assert file_parser_service.process_single_file(
        session_id, "a.csv", upload_id=old_id) is True

    # Must not have written cache or a terminal status for the new generation
    assert "a.csv" not in file_parser_service.load_cache(session_id)
    assert upload_state_store.get_file_status(session_id, "a.csv") == "queued"
    assert upload_state_store.get_upload_id(session_id, "a.csv") == new_id

    # The current generation's task succeeds normally
    assert file_parser_service.process_single_file(
        session_id, "a.csv", upload_id=new_id) is True
    assert upload_state_store.get_file_status(session_id, "a.csv") == "processed"
    assert "a.csv" in file_parser_service.load_cache(session_id)


def test_stale_task_does_not_touch_new_generation(session_id, monkeypatch):
    """A stale task is skipped before any parsing, so it can neither mark the
    NEW generation failed nor process it."""
    old_id = _register(session_id, "a.csv", "id,name\n1,x\n")
    session_manager.remove_file_from_session(session_id, "a.csv")
    new_id = _register(session_id, "a.csv", "id,name\n2,y\n")

    import app.services.file_parser_service as fps_mod

    def boom(*args, **kwargs):
        raise AssertionError("stale task must not parse the new file")

    monkeypatch.setattr(fps_mod, "preprocess_csv_file", boom)

    # Generation mismatch => no-op skip (returns True), parse never runs
    assert file_parser_service.process_single_file(
        session_id, "a.csv", upload_id=old_id) is True

    assert upload_state_store.get_upload_id(session_id, "a.csv") == new_id
    assert upload_state_store.get_file_status(session_id, "a.csv") == "queued"


# ---- cache write failures are terminal --------------------------------

def test_cache_save_failure_is_terminal_not_success(session_id, monkeypatch):
    upload_id = _register(session_id, "a.csv", "id,name\n1,x\n")

    monkeypatch.setattr(file_parser_service, "save_cache", lambda *a, **k: False)

    ok = file_parser_service.process_single_file(session_id, "a.csv", upload_id=upload_id)
    assert ok is False
    assert upload_state_store.get_file_status(session_id, "a.csv") == "failed"
    assert "cache" in upload_state_store.get_file(session_id, "a.csv")["error"].lower()


def test_cache_save_exception_is_terminal_not_raised(session_id, monkeypatch):
    upload_id = _register(session_id, "a.csv", "id,name\n1,x\n")

    def boom(*args, **kwargs):
        raise OSError("disk full")

    monkeypatch.setattr(file_parser_service, "save_cache", boom)

    ok = file_parser_service.process_single_file(session_id, "a.csv", upload_id=upload_id)
    assert ok is False  # must not propagate
    assert upload_state_store.get_file_status(session_id, "a.csv") == "failed"


# ---- delete vs. in-flight processing race -----------------------------

def test_delete_while_processing_never_resurrects_cache_entry(session_id):
    """Delete happens under the same cache lock that the worker uses for its
    cache write, so a worker that is mid-parse when the delete runs must not
    write the cache entry afterwards."""
    upload_id = _register(session_id, "a.csv", "id,name\n1,x\n")

    entered = threading.Event()
    release = threading.Event()

    import app.services.file_parser_service as fps_mod

    def blocking_preprocess(*args, **kwargs):
        entered.set()
        assert release.wait(timeout=10), "test timed out"
        return _ds("a.csv")

    monkeypatch = pytest.MonkeyPatch()
    monkeypatch.setattr(fps_mod, "preprocess_csv_file", blocking_preprocess)

    results = []

    def worker():
        try:
            results.append(file_parser_service.process_single_file(
                session_id, "a.csv", upload_id=upload_id))
        except Exception as exc:  # noqa: BLE001
            results.append(exc)

    t = threading.Thread(target=worker)
    t.start()
    try:
        assert entered.wait(timeout=10), "preprocess never started"

        # Exactly what the DELETE endpoint does: tombstone + cache removal
        # under a single lock.
        with file_parser_service.cache_lock(session_id):
            assert session_manager.remove_file_from_session(session_id, "a.csv") is True
            file_parser_service.remove_from_cache(session_id, "a.csv", lock_held=True)

        release.set()
        t.join(timeout=10)
    finally:
        release.set()
        if t.is_alive():
            t.join(timeout=5)
        monkeypatch.undo()

    assert t.is_alive() is False
    # Deleted while processing => no cache entry, no processed status
    assert "a.csv" not in file_parser_service.load_cache(session_id)
    assert upload_state_store.get_file_status(session_id, "a.csv") is None
    assert results[0] is True  # skipped (no-op), did not fail


# ---- TTL on key creation ----------------------------------------------

def test_per_file_keys_get_ttl_when_created(session_id):
    upload_state_store.add_file(session_id, "a.csv", 10, "2026-01-01T00:00:00+00:00")
    for key in (upload_state_store._files_key(session_id),
                upload_state_store._status_key(session_id),
                upload_state_store._order_key(session_id)):
        ttl = upload_state_store._redis.ttl(key)
        assert ttl is not None and ttl > 0, f"{key} has no TTL"

    upload_state_store.mark_deleted(session_id, "a.csv")
    ttl = upload_state_store._redis.ttl(upload_state_store._deleted_key(session_id))
    assert ttl is not None and ttl > 0, "deleted key has no TTL"


def test_metadata_update_preserves_generation_token(session_id):
    upload_id = upload_state_store.add_file(
        session_id, "a.csv", 10, "2026-01-01T00:00:00+00:00")
    upload_state_store.update_file_metadata(
        session_id, "a.csv", 1234, "2026-01-02T00:00:00+00:00")
    meta = upload_state_store.get_file(session_id, "a.csv")
    assert meta["size"] == 1234
    assert meta["upload_id"] == upload_id


# ---- persist-failure rollback -----------------------------------------

def test_rollback_file_upload_removes_registration_and_partial_file(session_id):
    upload_id = _register(session_id, "a.csv", "partial")
    path = os.path.join(session_manager._get_session_dir(session_id), "a.csv")
    assert os.path.exists(path)
    assert upload_state_store.get_upload_id(session_id, "a.csv") == upload_id

    session_manager.rollback_file_upload(session_id, "a.csv")

    assert upload_state_store.get_file(session_id, "a.csv") is None
    assert upload_state_store.is_deleted(session_id, "a.csv") is True
    assert not os.path.exists(path)
    assert "a.csv" not in session_manager.get_session(session_id).uploaded_files

    # The name can be re-uploaded afterwards (fresh generation)
    new_id = session_manager.add_file_to_session(session_id, "a.csv")
    assert new_id is not None and new_id != upload_id
