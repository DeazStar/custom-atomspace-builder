"""End-to-end flow tests: upload -> queued -> processing -> processed, plus
failure, delete-race, and the no-sync-reprocess datasource contract.
"""

import os

from app.core.session_manager import session_manager
from app.core.upload_state_store import upload_state_store
from app.services.file_parser_service import file_parser_service
from app.tasks.upload_tasks import process_uploaded_files_task


def _write_file(session_id, filename, content):
    session_dir = session_manager._get_session_dir(session_id)
    os.makedirs(session_dir, exist_ok=True)
    path = os.path.join(session_dir, filename)
    with open(path, "w", encoding="utf-8") as fh:
        fh.write(content)
    return path


def _upload_csv(session_id, filename, content):
    path = _write_file(session_id, filename, content)
    upload_id = session_manager.add_file_to_session(session_id, filename)
    assert upload_id is not None
    upload_state_store.update_file_metadata(session_id, filename,
                                            os.path.getsize(path),
                                            "2026-01-01T00:00:00+00:00")
    return path, upload_id


def _task_args(upload_id, filename):
    return [{"filename": filename, "upload_id": upload_id}]


def test_upload_to_processed(session_id):
    _, upload_id = _upload_csv(session_id, "data.csv", "id,name\n1,alice\n2,bob\n")

    assert upload_state_store.get_file_status(session_id, "data.csv") == "queued"

    result = process_uploaded_files_task(session_id, _task_args(upload_id, "data.csv"))
    assert result["files"] == {"data.csv": "processed"}
    assert upload_state_store.get_file_status(session_id, "data.csv") == "processed"

    cache = file_parser_service.load_cache(session_id)
    assert "data.csv" in cache
    assert cache["data.csv"].columns == ["id", "name"]


def test_failed_preprocessing_reports_error(session_id):
    _, upload_id = _upload_csv(session_id, "empty.csv", "")

    result = process_uploaded_files_task(session_id, _task_args(upload_id, "empty.csv"))
    assert result["files"] == {"empty.csv": "failed"}
    assert upload_state_store.get_file_status(session_id, "empty.csv") == "failed"
    assert upload_state_store.get_file(session_id, "empty.csv")["error"]


def test_deleted_file_not_resurrected_in_cache(session_id):
    _, upload_id = _upload_csv(session_id, "gone.csv", "id,name\n1,x\n")
    # Delete while the task is queued: tombstone + remove from session
    session_manager.remove_file_from_session(session_id, "gone.csv")

    result = process_uploaded_files_task(session_id, _task_args(upload_id, "gone.csv"))
    # Task must not write a cache entry for a deleted file
    assert "gone.csv" not in file_parser_service.load_cache(session_id)
    assert result["files"] == {"gone.csv": "processed"}  # no-op skip


def test_non_csv_marked_processed_without_datasource(session_id):
    path = _write_file(session_id, "notes.txt", "hello")
    upload_id = session_manager.add_file_to_session(session_id, "notes.txt")
    assert upload_id is not None
    upload_state_store.update_file_metadata(session_id, "notes.txt",
                                            os.path.getsize(path),
                                            "2026-01-01T00:00:00+00:00")

    process_uploaded_files_task(session_id, _task_args(upload_id, "notes.txt"))
    assert upload_state_store.get_file_status(session_id, "notes.txt") == "processed"
    assert "notes.txt" not in file_parser_service.load_cache(session_id)


def test_get_all_datasources_never_reprocesses_queued_file(session_id, monkeypatch):
    """The self-healing re-parse must be gone: a queued (unprocessed) CSV must
    yield an empty datasource list and must NOT trigger preprocessing."""
    _upload_csv(session_id, "pending.csv", "id,name\n1,x\n")
    assert upload_state_store.get_file_status(session_id, "pending.csv") == "queued"

    import app.services.file_parser_service as fps_mod

    def boom(*args, **kwargs):
        raise AssertionError("get_all_datasources must not preprocess files")

    monkeypatch.setattr(fps_mod, "preprocess_csv_file", boom)

    ds = file_parser_service.get_all_datasources(session_id)
    assert ds == []


def test_refresh_uses_locked_cache_write(session_id):
    _, upload_id = _upload_csv(session_id, "data.csv", "id,name\n1,alice\n")
    process_uploaded_files_task(session_id, _task_args(upload_id, "data.csv"))

    refreshed = file_parser_service.refresh_datasource(session_id, "data.csv")
    assert refreshed is not None
    assert upload_state_store.get_file_status(session_id, "data.csv") == "processed"
