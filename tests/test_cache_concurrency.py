"""Cache concurrency-safety tests.

Verifies that concurrent preprocessing of many files for the SAME session (as
happens when 4 Celery workers share a session) does not lose cache entries and
never leaves a partially-written cache file.
"""

import json
import os
from concurrent.futures import ThreadPoolExecutor

from app.core.session_manager import session_manager
from app.services.file_parser_service import file_parser_service


def _write_csv(session_id: str, filename: str, rows: int = 5, cols: int = 3):
    session_dir = session_manager._get_session_dir(session_id)
    os.makedirs(session_dir, exist_ok=True)
    path = os.path.join(session_dir, filename)
    lines = [",".join(f"col_{c}" for c in range(cols))]
    for r in range(rows):
        lines.append(",".join(f"r{r}c{c}" for c in range(cols)))
    with open(path, "w", encoding="utf-8") as fh:
        fh.write("\n".join(lines) + "\n")
    return path


def _register(session_id, filename, size):
    session_manager.add_file_to_session(session_id, filename)
    from app.core.upload_state_store import upload_state_store
    upload_state_store.update_file_metadata(session_id, filename, size,
                                            "2026-01-01T00:00:00+00:00")


def test_concurrent_processing_keeps_all_cache_entries(session_id):
    n = 20
    for i in range(n):
        _write_csv(session_id, f"f{i:03d}.csv")
        _register(session_id, f"f{i:03d}.csv", os.path.getsize(
            os.path.join(session_manager._get_session_dir(session_id), f"f{i:03d}.csv")))

    def work(i):
        return file_parser_service.process_single_file(session_id, f"f{i:03d}.csv")

    with ThreadPoolExecutor(max_workers=8) as pool:
        results = list(pool.map(work, range(n)))

    assert all(results), f"some files were not processed: {[i for i, ok in enumerate(results) if not ok]}"

    from app.core.upload_state_store import upload_state_store
    statuses = {f["filename"]: f["status"] for f in upload_state_store.get_files(session_id)}
    assert set(statuses.values()) == {"processed"}, statuses

    cache = file_parser_service.load_cache(session_id)
    assert len(cache) == n, f"expected {n} cache entries, got {len(cache)}"
    assert set(cache.keys()) == {f"f{i:03d}.csv" for i in range(n)}

    # Cache file must be a single valid JSON document
    cache_path = file_parser_service.get_cache_path(session_id)
    with open(cache_path, "r", encoding="utf-8") as fh:
        parsed = json.load(fh)
    assert len(parsed) == n
