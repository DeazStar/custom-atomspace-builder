"""Celery task that moves uploaded files out of the HTTP request path.

The worker reads the already-persisted CSV from the shared output filesystem,
runs the existing preprocessing logic, updates per-file state in Redis, and
writes the datasource cache safely (per-session lock + atomic replace).

Each entry carries the file's generation token (``upload_id``) captured at
enqueue time, so a task for a file that was deleted and re-uploaded under the
same name can never write cache/status for the newer file.
"""

from typing import Dict, List, Union

from ..core.celery_app import celery_app
from ..core.upload_state_store import upload_state_store
from ..services.file_parser_service import file_parser_service


@celery_app.task(bind=True, name="app.tasks.upload_tasks.process_uploaded_files_task")
def process_uploaded_files_task(self, session_id: str,
                                files: Union[List[str], List[Dict[str, str]]]) -> Dict[str, str]:
    """Preprocess each uploaded file and update shared state.

    ``files`` is a list of ``{"filename": ..., "upload_id": ...}`` dicts. A
    legacy ``list[str]`` of filenames is accepted too (no generation token).

    Returns a mapping filename -> terminal status ("processed" | "failed").
    Each file is handled independently so one bad file cannot abort the rest.
    """
    entries: List[Dict[str, str]] = []
    for item in files:
        if isinstance(item, str):
            entries.append({"filename": item, "upload_id": ""})
        else:
            entries.append(item)

    results: Dict[str, str] = {}
    for entry in entries:
        filename = entry["filename"]
        upload_id = entry.get("upload_id") or None
        try:
            ok = file_parser_service.process_single_file(
                session_id, filename, upload_id=upload_id)
            results[filename] = "processed" if ok else "failed"
        except Exception as exc:  # noqa: BLE001 - surface as failed, keep going
            try:
                upload_state_store.set_file_status(session_id, filename, "failed", error=str(exc))
            except Exception:  # noqa: BLE001
                # Terminal state could not be persisted (e.g. Redis down). Acking
                # this task would leave the file stuck in queued/processing with
                # no way to recover, so re-raise and let Celery's acks_late
                # behavior redeliver the task.
                raise
            results[filename] = "failed"
    return {"session_id": session_id, "files": results}
