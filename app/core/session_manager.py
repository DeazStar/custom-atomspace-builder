"""Upload session management.

Sessions and per-file state live in Redis (``app/core/upload_state_store.py``)
so the FastAPI process and Celery workers agree on uploads/status. Physical
files live on the shared output filesystem under ``{output_dir}/uploads/{id}``.
The public API of this class is preserved for callers that construct
``UploadSession`` objects.
"""

import os
import shutil
import secrets
from datetime import datetime, timezone
from typing import Optional
from ..models.schemas import UploadSession
from ..models.enums import SessionStatus
from ..config import settings
from .upload_state_store import upload_state_store


class SessionManager:
    """Manages upload sessions with automatic cleanup."""

    def __init__(self):
        pass

    def create_session(self) -> str:
        """Create a new upload session (state in Redis, dir on shared disk)."""
        session_id = secrets.token_urlsafe(32)
        now = datetime.now(tz=timezone.utc)
        expires_at = now + settings.session_timeout
        upload_state_store.create_session(
            session_id,
            created_at=now,
            expires_at=expires_at,
            status=SessionStatus.ACTIVE.value
        )

        # Create session directory
        session_dir = self._get_session_dir(session_id)
        os.makedirs(session_dir, exist_ok=True)

        return session_id

    def get_session(self, session_id: str) -> Optional[UploadSession]:
        """Get an active session by ID (reconstructed from Redis)."""
        state = upload_state_store.get_session(session_id)

        if not state:
            return None

        return UploadSession(
            session_id=state["session_id"],
            created_at=datetime.fromisoformat(state["created_at"]),
            expires_at=datetime.fromisoformat(state["expires_at"]),
            uploaded_files=list(state.get("uploaded_files", [])),
            status=state.get("status", SessionStatus.ACTIVE.value),
            metadata={}
        )

    def add_file_to_session(self, session_id: str, filename: str, size: int = 0,
                            uploaded_at: str = None, status: str = "queued") -> Optional[str]:
        """Add a file to an existing session (atomic duplicate detection).

        Returns the file's generation token (``upload_id``) on success, or None
        if the file could not be added (session missing or duplicate name).
        """
        session = self.get_session(session_id)
        if not session:
            return None

        uploaded_at = uploaded_at or datetime.now(tz=timezone.utc).isoformat()
        return upload_state_store.add_file(session_id, filename, size, uploaded_at, status=status)

    def rollback_file_upload(self, session_id: str, filename: str) -> None:
        """Undo a failed upload: remove the registration, tombstone the name so
        no queued task processes it, and delete the partial file on disk."""
        upload_state_store.mark_deleted(session_id, filename)
        file_path = os.path.join(self._get_session_dir(session_id), filename)
        if os.path.exists(file_path):
            os.remove(file_path)

    def remove_file_from_session(self, session_id: str, filename: str) -> bool:
        """Remove a file from a session (tombstone + physical delete)."""
        session = self.get_session(session_id)
        if not session:
            return False

        if filename not in session.uploaded_files:
            return False

        upload_state_store.mark_deleted(session_id, filename)

        # Remove physical file
        file_path = os.path.join(self._get_session_dir(session_id), filename)
        if os.path.exists(file_path):
            os.remove(file_path)

        return True

    def consume_session(self, session_id: str) -> bool:
        """Mark session as consumed (used for job processing)."""
        if not self.get_session(session_id):
            return False

        upload_state_store.set_session_status(session_id, SessionStatus.CONSUMED.value)
        return True

    def cleanup_session(self, session_id: str):
        """Clean up session files, directory, and Redis state."""
        session_dir = self._get_session_dir(session_id)
        if os.path.exists(session_dir):
            shutil.rmtree(session_dir, ignore_errors=True)

        upload_state_store.delete_session(session_id)

    def cleanup_expired_sessions(self) -> int:
        """Clean up all expired sessions and return count."""
        expired_sessions = upload_state_store.cleanup_expired()

        for session_id in expired_sessions:
            self.cleanup_session(session_id)

        return len(expired_sessions)

    def _get_session_dir(self, session_id: str) -> str:
        """Get the directory path for a session."""
        return os.path.join(settings.base_output_dir, "uploads", session_id)

    def get_session_files_info(self, session_id: str) -> list:
        """Get information about files in a session (from Redis state)."""
        session = self.get_session(session_id)
        if not session:
            return []

        return [
            {
                "filename": f["filename"],
                "size": f.get("size", 0),
                "uploaded_at": f.get("uploaded_at", ""),
                "status": f.get("status", "queued"),
                "error": f.get("error", ""),
            }
            for f in upload_state_store.get_files(session_id)
        ]


# Global session manager instance
session_manager = SessionManager()
