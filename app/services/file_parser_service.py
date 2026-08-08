"""File parsing service for handling CSV preprocessing and caching.

Preprocessing runs in Celery workers, never in the FastAPI request/event-loop
path. The API only persists files, records per-file state in Redis, and enqueues
the preprocessing task. All cache read-modify-write operations on
``datasources_cache.json`` are guarded by a per-session Redis lock and written
atomically (temp file + ``os.replace``) so concurrent workers cannot clobber
each other's entries.
"""

import os
import json
import tempfile
from contextlib import contextmanager, nullcontext
from typing import Dict, List, Optional
from datetime import datetime, timezone

from ..models.schemas import DataSource, FileInfo
from ..core.session_manager import session_manager
from ..core.upload_state_store import upload_state_store
from ..utils.file_utils import (
    preprocess_csv_file, 
    is_csv_file
)


def is_error_datasource(datasource: DataSource) -> bool:
    """True when a datasource represents a failed parse (create_error_datasource)."""
    return datasource.id.startswith("ds_error_")


class FileParserService:
    """Service for parsing and caching file data sources."""
    
    CACHE_FILENAME = "datasources_cache.json"
    
    def __init__(self):
        """Initialize the file parser service."""
        pass
    
    def get_cache_path(self, session_id: str) -> str:
        """Get the path to the datasources cache file for a session."""
        session_dir = session_manager._get_session_dir(session_id)
        return os.path.join(session_dir, self.CACHE_FILENAME)
    
    @contextmanager
    def cache_lock(self, session_id: str):
        """Acquire the per-session cache lock (Redis), scoped to a context."""
        with upload_state_store.cache_lock(session_id) as lock:
            yield lock
    
    def load_cache(self, session_id: str) -> Dict[str, DataSource]:
        """Load cached datasources from JSON file."""
        cache_path = self.get_cache_path(session_id)
        
        if not os.path.exists(cache_path):
            return {}
        
        try:
            with open(cache_path, 'r', encoding='utf-8') as f:
                cache_data = json.load(f)
                
            # Convert back to DataSource objects
            datasources = {}
            for filename, data in cache_data.items():
                datasources[filename] = DataSource(**data)
                
            return datasources
        except Exception as e:
            print(f"Error loading datasources cache for session {session_id}: {str(e)}")
            return {}
    
    def save_cache(self, session_id: str, datasources: Dict[str, DataSource]) -> bool:
        """Save datasources cache to JSON file atomically (temp file + replace).

        Concurrent readers therefore never observe a partially written file, and
        concurrent writers cannot corrupt the file (last replace wins atomically;
        cross-process entry merging is guarded by the per-session lock).
        """
        cache_path = self.get_cache_path(session_id)
        
        try:
            # Ensure session directory exists
            os.makedirs(os.path.dirname(cache_path), exist_ok=True)
            
            # Convert DataSource objects to dict for JSON serialization
            cache_data = {}
            for filename, datasource in datasources.items():
                cache_data[filename] = datasource.model_dump()
            
            fd, tmp_path = tempfile.mkstemp(
                dir=os.path.dirname(cache_path),
                prefix=f".{self.CACHE_FILENAME}.",
                suffix=".tmp"
            )
            try:
                with os.fdopen(fd, 'w', encoding='utf-8') as f:
                    json.dump(cache_data, f, indent=2, ensure_ascii=False)
                os.replace(tmp_path, cache_path)
            except Exception:
                if os.path.exists(tmp_path):
                    os.remove(tmp_path)
                raise
            
            return True
        except Exception as e:
            print(f"Error saving datasources cache for session {session_id}: {str(e)}")
            return False
    
    def _file_still_present(self, session_id: str, filename: str,
                            upload_id: Optional[str] = None) -> bool:
        """A file is writable into the cache only if it still exists in the
        session, was not deleted (tombstoned), and still belongs to the same
        upload generation (not deleted and re-uploaded) that this task was
        enqueued for."""
        if upload_state_store.is_deleted(session_id, filename):
            return False
        meta = upload_state_store.get_file(session_id, filename)
        if meta is None:
            return False
        if upload_id is not None and meta.get("upload_id") != upload_id:
            return False
        return True

    def _set_status_if_current(self, session_id: str, filename: str,
                               upload_id: Optional[str], status: str,
                               error: Optional[str] = None) -> bool:
        """Set a file's status only if it still belongs to the upload generation
        this task was enqueued for. Returns False (no-op) when the file was
        deleted or re-uploaded since the task started."""
        if upload_id is not None:
            current_id = upload_state_store.get_upload_id(session_id, filename)
            if current_id != upload_id:
                return False
        return upload_state_store.set_file_status(session_id, filename, status,
                                                  error=error)

    def process_single_file(self, session_id: str, filename: str,
                            upload_id: Optional[str] = None) -> bool:
        """Preprocess one file and record its terminal state.

        Runs in a Celery worker. Returns True when the file reaches a terminal
        state (processed / skipped), False on failure (status becomes 'failed').
        Deleted, already-terminal, or stale-generation (``upload_id`` no longer
        matches) files are no-ops. Never raises for a per-file failure.
        """
        if upload_state_store.is_deleted(session_id, filename):
            return True
        if upload_state_store.get_file(session_id, filename) is None:
            return True
        if upload_id is not None and \
                upload_state_store.get_upload_id(session_id, filename) != upload_id:
            return True

        current = upload_state_store.get_file_status(session_id, filename)
        if current in ("processed", "failed", "deleted"):
            return True

        self._set_status_if_current(session_id, filename, upload_id, "processing")

        # Non-CSV files need no preprocessing
        if not is_csv_file(filename):
            self._set_status_if_current(session_id, filename, upload_id, "processed")
            return True

        session_dir = session_manager._get_session_dir(session_id)
        file_path = os.path.join(session_dir, filename)
        if not os.path.exists(file_path):
            self._set_status_if_current(
                session_id, filename, upload_id, "failed", error="file missing on disk")
            return False

        try:
            datasource = preprocess_csv_file(file_path, filename)
        except Exception as e:
            self._set_status_if_current(
                session_id, filename, upload_id, "failed",
                error=f"Processing error: {str(e)}")
            return False

        # Write the datasource into the cache under the session lock, but only
        # if the file was not deleted/re-uploaded while this task was queued,
        # and only if the cache write actually succeeds (a failed write must not
        # be reported as success). Cache failures are terminal, never raised.
        try:
            with self.cache_lock(session_id):
                if not self._file_still_present(session_id, filename, upload_id):
                    return True
                cached = self.load_cache(session_id)
                cached[filename] = datasource
                if not self.save_cache(session_id, cached):
                    raise IOError("datasource cache save failed")
        except Exception as e:
            self._set_status_if_current(
                session_id, filename, upload_id, "failed",
                error=f"Failed to write datasource cache: {str(e)}")
            return False

        if is_error_datasource(datasource):
            error = datasource.sampleRow[0] if datasource.sampleRow else "Invalid CSV structure"
            self._set_status_if_current(session_id, filename, upload_id, "failed", error=error)
            return False

        self._set_status_if_current(session_id, filename, upload_id, "processed")
        return True
    
    def update_cache_with_new_files(self, session_id: str, new_files: List[str]) -> bool:
        """Process newly uploaded files and update the cache (worker-oriented)."""
        if not new_files:
            return True
        
        all_ok = True
        for filename in new_files:
            ok = self.process_single_file(session_id, filename)
            if not ok:
                all_ok = False
        return all_ok
    
    def remove_from_cache(self, session_id: str, filename: str,
                          lock_held: bool = False) -> bool:
        """Remove a file's datasource from cache when file is deleted.

        ``lock_held=True`` skips acquiring the per-session lock, for callers
        that already hold it (e.g. the delete endpoint, which tombstones the
        file and removes the cache entry under a single lock so an in-flight
        worker cannot interleave). Returns False if the cache write fails.
        """
        context = self.cache_lock(session_id) if not lock_held else nullcontext()
        with context:
            cached_datasources = self.load_cache(session_id)

            if filename in cached_datasources:
                del cached_datasources[filename]
                return self.save_cache(session_id, cached_datasources)

        return True
    
    def get_all_datasources(self, session_id: str) -> List[DataSource]:
        """Get datasources that have already been preprocessed.

        This MUST NOT reprocess queued/processing files in the request path.
        It returns cache entries for files still present in the session; the
        status endpoints report how far each file has progressed.
        """
        session = session_manager.get_session(session_id)
        if not session:
            return []
        
        # Load cached datasources
        cached_datasources = self.load_cache(session_id)
        
        # Filter out datasources for files that no longer exist in session
        valid_datasources = []
        for filename in session.uploaded_files:
            if filename in cached_datasources:
                valid_datasources.append(cached_datasources[filename])
        
        return valid_datasources
    
    def process_uploaded_files(self, session_id: str, filenames: List[str]) -> bool:
        """Process multiple uploaded files and update cache."""
        return self.update_cache_with_new_files(session_id, filenames)
    
    def get_datasource_by_filename(self, session_id: str, filename: str) -> Optional[DataSource]:
        """Get a specific datasource by filename."""
        cached_datasources = self.load_cache(session_id)
        return cached_datasources.get(filename)
    
    def refresh_datasource(self, session_id: str, filename: str) -> Optional[DataSource]:
        """Force refresh a specific datasource by reprocessing the file."""
        if not is_csv_file(filename):
            return None
        
        session_dir = session_manager._get_session_dir(session_id)
        file_path = os.path.join(session_dir, filename)
        
        if not os.path.exists(file_path):
            return None
        
        # Reprocess the file
        datasource = preprocess_csv_file(file_path, filename)
        if datasource:
            with self.cache_lock(session_id):
                cached_datasources = self.load_cache(session_id)
                cached_datasources[filename] = datasource
                if not self.save_cache(session_id, cached_datasources):
                    return None

            if is_error_datasource(datasource):
                error = datasource.sampleRow[0] if datasource.sampleRow else "Invalid CSV structure"
                upload_state_store.set_file_status(
                    session_id, filename, "failed", error=error)
            else:
                upload_state_store.set_file_status(session_id, filename, "processed")
        
        return datasource
    
    def clear_cache(self, session_id: str) -> bool:
        """Clear all cached datasources for a session."""
        cache_path = self.get_cache_path(session_id)
        try:
            with self.cache_lock(session_id):
                if os.path.exists(cache_path):
                    os.remove(cache_path)
            return True
        except Exception as e:
            print(f"Error clearing cache for session {session_id}: {str(e)}")
            return False
    
    def get_cache_stats(self, session_id: str) -> Dict[str, any]:
        """Get cache statistics for debugging."""
        cache_path = self.get_cache_path(session_id)
        
        stats = {
            "cache_exists": os.path.exists(cache_path),
            "cache_path": cache_path,
            "cached_files_count": 0,
            "cache_size_bytes": 0,
            "last_modified": None
        }
        
        if os.path.exists(cache_path):
            try:
                cache_stat = os.stat(cache_path)
                stats["cache_size_bytes"] = cache_stat.st_size
                stats["last_modified"] = datetime.fromtimestamp(
                    cache_stat.st_mtime, tz=timezone.utc
                ).isoformat()
                
                cached_datasources = self.load_cache(session_id)
                stats["cached_files_count"] = len(cached_datasources)
            except Exception as e:
                stats["error"] = str(e)
        
        return stats


# Global instance
file_parser_service = FileParserService()
