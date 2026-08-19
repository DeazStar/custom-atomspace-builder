"""Redis-backed shared upload/session state for the API and Celery workers.

The API and the Celery workers run in separate processes, so the process-local
in-memory ``SessionManager.sessions`` dict can no longer be the source of truth.
This store keeps the minimal state both sides need to agree on:

- session metadata (created/expires/status)
- per-file metadata (size, uploaded time) in one hash and the mutable
  processing state (status/error) in a second hash, so a status update is a
  single atomic HSET that never clobbers the metadata
- a generation token (``upload_id``) per upload, so a stale Celery task (from a
  file that was deleted and re-uploaded under the same name) can never write
  cache/status for the newer file
- an ordered list of uploaded filenames
- a tombstone set for files deleted while queued/processing (so a worker that
  finishes late must not resurrect cache entries for a deleted file)

Status values follow the upload contract:
``uploaded | queued | processing | processed | failed | deleted``
"""

import json
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import redis

from ..config import settings


class StateStoreUnavailable(Exception):
    """Raised when Redis state operations fail (broker/state down)."""


class UploadStateStore:
    """Redis-backed per-session and per-file upload state."""

    def __init__(self, redis_url: Optional[str] = None):
        self._redis_url = redis_url or settings.redis_url
        self._redis = redis.Redis.from_url(self._redis_url, decode_responses=True)

    # ---- key helpers ---------------------------------------------------
    @staticmethod
    def _meta_key(session_id: str) -> str:
        return f"upload:{session_id}:meta"

    @staticmethod
    def _files_key(session_id: str) -> str:
        return f"upload:{session_id}:files"

    @staticmethod
    def _status_key(session_id: str) -> str:
        return f"upload:{session_id}:status"

    @staticmethod
    def _order_key(session_id: str) -> str:
        return f"upload:{session_id}:order"

    @staticmethod
    def _deleted_key(session_id: str) -> str:
        return f"upload:{session_id}:deleted"

    INDEX_KEY = "upload:index"

    def _ttl_seconds(self, expires_at: datetime) -> int:
        """Seconds until expiry (used for Redis EXPIRE)."""
        delta = expires_at - datetime.now(tz=timezone.utc)
        return max(1, int(delta.total_seconds()))

    def _apply_session_ttl(self, session_id: str, *keys: str) -> None:
        """Apply the session's remaining TTL to keys that were just created.

        ``create_session`` cannot set a TTL on the per-file keys because they do
        not exist yet (EXPIRE is a no-op for missing keys), so the TTL must be
        (re)applied whenever one of those keys is actually created.
        """
        meta = self._redis.hgetall(self._meta_key(session_id))
        if not meta or "expires_at" not in meta:
            return
        ttl = self._ttl_seconds(datetime.fromisoformat(meta["expires_at"]))
        for key in keys:
            self._redis.expire(key, ttl)

    # ---- session lifecycle ---------------------------------------------
    def create_session(self, session_id: str, created_at: datetime,
                       expires_at: datetime, status: str = "active") -> bool:
        """Persist a new session. Returns True on success.

        The session TTL is applied to the meta key here. Per-file keys
        (files/status/order/deleted) do not exist yet, so their TTL is applied
        when they are created (see ``_apply_session_ttl``) instead of relying
        on an EXPIRE that Redis silently no-ops for missing keys.
        """
        try:
            pipe = self._redis.pipeline(transaction=True)
            meta = {
                "session_id": session_id,
                "created_at": created_at.isoformat(),
                "expires_at": expires_at.isoformat(),
                "status": status,
            }
            pipe.hset(self._meta_key(session_id), mapping=meta)
            pipe.sadd(self.INDEX_KEY, session_id)
            ttl = self._ttl_seconds(expires_at)
            pipe.expire(self._meta_key(session_id), ttl)
            pipe.execute()
            return True
        except redis.RedisError as e:
            raise StateStoreUnavailable(str(e))

    def get_session(self, session_id: str) -> Optional[Dict[str, Any]]:
        """Return session state (meta + ordered uploaded filenames) or None.

        Expired sessions are removed from Redis and reported as missing,
        mirroring the old in-memory expiry behavior.
        """
        try:
            meta = self._redis.hgetall(self._meta_key(session_id))
            if not meta:
                return None

            expires_at = datetime.fromisoformat(meta["expires_at"])
            if expires_at <= datetime.now(tz=timezone.utc):
                self.delete_session(session_id)
                return None

            filenames = self._redis.lrange(self._order_key(session_id), 0, -1)
            return {**meta, "uploaded_files": list(filenames)}
        except redis.RedisError as e:
            raise StateStoreUnavailable(str(e))

    def set_session_status(self, session_id: str, status: str) -> bool:
        try:
            return bool(self._redis.hset(self._meta_key(session_id), "status", status))
        except redis.RedisError as e:
            raise StateStoreUnavailable(str(e))

    def delete_session(self, session_id: str) -> None:
        """Remove every key owned by a session."""
        try:
            keys = [k(session_id) for k in (
                self._meta_key, self._files_key, self._status_key,
                self._order_key, self._deleted_key)]
            self._redis.delete(*keys)
            self._redis.srem(self.INDEX_KEY, session_id)
        except redis.RedisError as e:
            raise StateStoreUnavailable(str(e))

    def list_session_ids(self) -> List[str]:
        try:
            return list(self._redis.smembers(self.INDEX_KEY))
        except redis.RedisError as e:
            raise StateStoreUnavailable(str(e))

    def cleanup_expired(self) -> List[str]:
        """Delete expired sessions and return their ids."""
        now = datetime.now(tz=timezone.utc)
        expired = []
        for session_id in self.list_session_ids():
            meta = self._redis.hgetall(self._meta_key(session_id))
            if not meta:
                self._redis.srem(self.INDEX_KEY, session_id)
                continue
            try:
                expires_at = datetime.fromisoformat(meta["expires_at"])
            except (KeyError, ValueError):
                continue
            if expires_at <= now:
                self.delete_session(session_id)
                expired.append(session_id)
        return expired

    # ---- per-file state ------------------------------------------------
    def add_file(self, session_id: str, filename: str, size: int,
                 uploaded_at: str, status: str = "queued") -> Optional[str]:
        """Register a file in the session.

        Each successful registration creates a NEW generation token
        (``upload_id``). The token is returned and must be carried by the Celery
        task so stale tasks (from a file deleted and re-uploaded under the same
        name) can never write cache/status for the newer file.

        Uses HSETNX so concurrent duplicate uploads are detected atomically.
        Returns the new ``upload_id``, or None if the file already existed.
        """
        upload_id = uuid.uuid4().hex
        files_meta = {
            "filename": filename,
            "size": size,
            "uploaded_at": uploaded_at,
            "upload_id": upload_id,
        }
        status_meta = {"status": status, "error": "", "upload_id": upload_id}
        try:
            added = self._redis.hsetnx(self._files_key(session_id), filename,
                                       json.dumps(files_meta))
            if added:
                pipe = self._redis.pipeline(transaction=True)
                pipe.hset(self._status_key(session_id), filename,
                          json.dumps(status_meta))
                pipe.srem(self._deleted_key(session_id), filename)
                pipe.rpush(self._order_key(session_id), filename)
                pipe.execute()
                self._apply_session_ttl(
                    session_id,
                    self._files_key(session_id),
                    self._status_key(session_id),
                    self._order_key(session_id),
                )
                return upload_id
            return None
        except redis.RedisError as e:
            raise StateStoreUnavailable(str(e))

    def get_file(self, session_id: str, filename: str) -> Optional[Dict[str, Any]]:
        try:
            raw_files = self._redis.hget(self._files_key(session_id), filename)
            raw_status = self._redis.hget(self._status_key(session_id), filename)
            if raw_files is None:
                return None
            meta = json.loads(raw_files)
            if raw_status is not None:
                meta.update(json.loads(raw_status))
            return meta
        except redis.RedisError as e:
            raise StateStoreUnavailable(str(e))

    def update_file_metadata(self, session_id: str, filename: str, size: int,
                             uploaded_at: str) -> bool:
        """Update the immutable-ish metadata of an already-registered file.

        Called after the file is fully persisted (size becomes known). The
        duplicate check is done by ``add_file``'s HSETNX, so an HSET here only
        ever updates this file's own record. The generation token is preserved.
        """
        try:
            current = self._redis.hget(self._files_key(session_id), filename)
            upload_id = None
            if current:
                upload_id = json.loads(current).get("upload_id")
            return bool(self._redis.hset(
                self._files_key(session_id), filename,
                json.dumps({
                    "filename": filename,
                    "size": size,
                    "uploaded_at": uploaded_at,
                    "upload_id": upload_id,
                })))
        except redis.RedisError as e:
            raise StateStoreUnavailable(str(e))

    def get_upload_id(self, session_id: str, filename: str) -> Optional[str]:
        """Return the generation token for a registered file (None if absent)."""
        meta = self.get_file(session_id, filename)
        return meta.get("upload_id") if meta else None

    def get_file_status(self, session_id: str, filename: str) -> Optional[str]:
        try:
            raw = self._redis.hget(self._status_key(session_id), filename)
            if raw is None:
                return None
            return json.loads(raw).get("status")
        except redis.RedisError as e:
            raise StateStoreUnavailable(str(e))

    def set_file_status(self, session_id: str, filename: str, status: str,
                        error: Optional[str] = None) -> bool:
        try:
            return bool(self._redis.hset(
                self._status_key(session_id), filename,
                json.dumps({"status": status, "error": error or ""})))
        except redis.RedisError as e:
            raise StateStoreUnavailable(str(e))

    def get_files(self, session_id: str) -> List[Dict[str, Any]]:
        """Return per-file metadata + status in insertion order."""
        try:
            files_key = self._files_key(session_id)
            status_key = self._status_key(session_id)
            filenames = self._redis.lrange(self._order_key(session_id), 0, -1)
            if not filenames:
                return []
            with self._redis.pipeline(transaction=False) as pipe:
                for name in filenames:
                    pipe.hget(files_key, name)
                    pipe.hget(status_key, name)
                raws = pipe.execute()
            files = []
            for i, name in enumerate(filenames):
                raw_files = raws[2 * i]
                raw_status = raws[2 * i + 1]
                if raw_files is None:
                    continue
                meta = json.loads(raw_files)
                if raw_status is not None:
                    meta.update(json.loads(raw_status))
                files.append(meta)
            return files
        except redis.RedisError as e:
            raise StateStoreUnavailable(str(e))

    # ---- deletion / tombstones -----------------------------------------
    def mark_deleted(self, session_id: str, filename: str) -> bool:
        """Tombstone a file so a late worker must not recreate cache data."""
        try:
            pipe = self._redis.pipeline(transaction=True)
            pipe.sadd(self._deleted_key(session_id), filename)
            pipe.hdel(self._files_key(session_id), filename)
            pipe.hdel(self._status_key(session_id), filename)
            pipe.lrem(self._order_key(session_id), 0, filename)
            pipe.execute()
            self._apply_session_ttl(session_id, self._deleted_key(session_id))
            return True
        except redis.RedisError as e:
            raise StateStoreUnavailable(str(e))

    def is_deleted(self, session_id: str, filename: str) -> bool:
        try:
            return bool(self._redis.sismember(self._deleted_key(session_id), filename))
        except redis.RedisError as e:
            raise StateStoreUnavailable(str(e))

    # ---- distributed lock for the datasource cache ---------------------
    def cache_lock(self, session_id: str, timeout: int = 60, blocking_timeout: int = 15):
        """Redis lock scoped to a session's datasource cache read-modify-write."""
        return self._redis.lock(
            f"upload_cache_lock:{session_id}",
            timeout=timeout,
            blocking_timeout=blocking_timeout,
        )


upload_state_store = UploadStateStore()
