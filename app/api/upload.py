"""Upload-related API endpoints with refactored file parsing service.

The upload request path only validates the session, streams files to disk,
records per-file state in Redis, and enqueues a Celery task. CSV preprocessing
runs in Celery workers; status/datasource endpoints report per-file progress and
never re-run preprocessing in the request/event-loop path.
"""

import os
from datetime import datetime, timezone
from typing import List
from fastapi import APIRouter, File, UploadFile, Form, HTTPException
from ..config import settings
from ..core.session_manager import session_manager
from ..core.upload_state_store import upload_state_store
from ..services.file_parser_service import file_parser_service
from ..tasks.upload_tasks import process_uploaded_files_task
from ..models.schemas import (
    CreateSessionResponse, 
    UploadResponse, 
    SessionStatusResponse,
    UploadFileInfo,
    DataSource
)

router = APIRouter(prefix="/api/upload", tags=["upload"])


@router.post("/create-session", response_model=CreateSessionResponse)
async def create_upload_session():
    """Create a new upload session."""
    session_id = session_manager.create_session()
    session = session_manager.get_session(session_id)
    
    return CreateSessionResponse(
        session_id=session_id,
        expires_at=session.expires_at.isoformat(),
        upload_url=f"/api/upload/files"
    )


@router.post("/files", response_model=UploadResponse)
async def upload_files(
    session_id: str = Form(...),
    files: List[UploadFile] = File(...)
):
    """Upload files to a specific session.

    Persists each file in chunks, records per-file state as 'queued', enqueues
    the preprocessing task, and returns immediately (no CSV parsing here).
    """
    session = session_manager.get_session(session_id)
    if not session:
        raise HTTPException(status_code=404, detail="Session not found or expired")
    
    session_dir = session_manager._get_session_dir(session_id)
    uploaded_files = []
    pending_entries = []
    # Files registered by THIS request (each a fresh generation token). On any
    # failure mid-request we roll back exactly these - never files that existed
    # before the request.
    registered_in_request = []

    try:
        for file in files:
            # Register first (atomic HSETNX in the state store). The returned
            # upload_id is the generation token carried by the Celery task.
            upload_id = session_manager.add_file_to_session(session_id, file.filename)
            if not upload_id:
                raise HTTPException(
                    status_code=400, 
                    detail=f"File {file.filename} already uploaded"
                )
            registered_in_request.append({"filename": file.filename, "upload_id": upload_id})

            file_path = os.path.join(session_dir, file.filename)

            try:
                with open(file_path, "wb") as f:
                    while True:
                        chunk = await file.read(settings.upload_chunk_size)
                        if not chunk:
                            break
                        f.write(chunk)

                size = os.path.getsize(file_path)
                uploaded_at = datetime.now(tz=timezone.utc).isoformat()
                # Record size/uploaded_at once the file is fully persisted
                upload_state_store.update_file_metadata(
                    session_id, file.filename, size, uploaded_at)

                uploaded_files.append(UploadFileInfo(
                    filename=file.filename,
                    size=size,
                    uploaded_at=uploaded_at
                ))
                pending_entries.append({"filename": file.filename, "upload_id": upload_id})

            except Exception as e:
                # Persist failed for this file: raise so the whole request is
                # rolled back below (the failing file is in registered_in_request).
                raise HTTPException(
                    status_code=500, 
                    detail=f"Failed to upload {file.filename}: {str(e)}"
                ) from e
    except Exception:
        # A partial multi-file upload must never leave earlier files registered
        # as 'queued' with no task to process them (enqueue only happens after
        # the full loop). Roll back every file registered by THIS request.
        for entry in registered_in_request:
            try:
                session_manager.rollback_file_upload(session_id, entry["filename"])
            except Exception:  # noqa: BLE001 - best-effort rollback
                pass
        raise
    
    # Enqueue background preprocessing
    task_id = None
    try:
        if pending_entries:
            task = process_uploaded_files_task.delay(session_id, pending_entries)
            task_id = task.id
    except Exception as e:
        for entry in pending_entries:
            upload_state_store.set_file_status(
                session_id, entry["filename"], "failed",
                error=f"Failed to enqueue preprocessing: {str(e)}")
        raise HTTPException(
            status_code=503,
            detail=f"Failed to enqueue preprocessing task: {str(e)}"
        )
    
    # Re-fetch session so files_in_session reflects the committed state
    session = session_manager.get_session(session_id)
    new_filenames = [entry["filename"] for entry in pending_entries]
    
    return UploadResponse(
        session_id=session_id,
        uploaded_files=uploaded_files,
        total_files=len(session.uploaded_files) if session else len(new_filenames),
        files_in_session=session.uploaded_files if session else [],
        task_id=task_id,
        file_statuses={filename: "queued" for filename in new_filenames}
    )


@router.get("/{session_id}/status", response_model=SessionStatusResponse)
async def get_upload_status(session_id: str):
    """Get upload session status, file list, and per-file processing state."""
    session = session_manager.get_session(session_id)
    if not session:
        raise HTTPException(status_code=404, detail="Session not found or expired")
    
    # Original file details (includes uploaded_at + processing status)
    file_details = session_manager.get_session_files_info(session_id)
    file_statuses = {f["filename"]: f["status"] for f in file_details}
    errors = {f["filename"]: f["error"] for f in file_details if f.get("error")}
    
    # Datasources that have already been preprocessed (no re-parsing here)
    datasources = file_parser_service.get_all_datasources(session_id)
    
    return SessionStatusResponse(
        session_id=session_id,
        status=session.status,
        expires_at=session.expires_at.isoformat(),
        files=[UploadFileInfo(
            filename=f["filename"],
            size=f["size"],
            uploaded_at=f["uploaded_at"]
        ) for f in file_details],
        file_statuses=file_statuses,
        errors=errors,
        total_files=len(file_details),
        datasources=datasources
    )


@router.delete("/{session_id}/files/{filename}")
async def delete_uploaded_file(session_id: str, filename: str):
    """Remove a file from upload session."""
    session = session_manager.get_session(session_id)
    if not session:
        raise HTTPException(status_code=404, detail="Session not found or expired")
    
    if filename not in session.uploaded_files:
        raise HTTPException(status_code=404, detail="File not found in session")
    
    # Tombstone the file and remove its cache entry under ONE session cache
    # lock, so an in-flight worker cannot write the cache entry after we removed
    # it (nor resurrect a deleted file). Cache removal is best-effort cleanup:
    # get_all_datasources only surfaces entries for files still in the session.
    with file_parser_service.cache_lock(session_id):
        if not session_manager.remove_file_from_session(session_id, filename):
            raise HTTPException(status_code=500, detail="Failed to remove file")
        file_parser_service.remove_from_cache(session_id, filename, lock_held=True)
    
    return {"message": f"File {filename} removed successfully"}


# Endpoint to get data sources only
@router.get("/{session_id}/datasources", response_model=List[DataSource])
async def get_session_datasources(session_id: str):
    """Get the data sources that have already been preprocessed for a session."""
    session = session_manager.get_session(session_id)
    if not session:
        raise HTTPException(status_code=404, detail="Session not found or expired")
    
    return file_parser_service.get_all_datasources(session_id)


# Additional utility endpoints
@router.post("/{session_id}/refresh/{filename}")
async def refresh_datasource(session_id: str, filename: str):
    """Force refresh a specific datasource by reprocessing the file."""
    session = session_manager.get_session(session_id)
    if not session:
        raise HTTPException(status_code=404, detail="Session not found or expired")
    
    if filename not in session.uploaded_files:
        raise HTTPException(status_code=404, detail="File not found in session")
    
    datasource = file_parser_service.refresh_datasource(session_id, filename)
    if not datasource:
        raise HTTPException(status_code=400, detail="Failed to refresh datasource")
    
    status = "failed" if datasource.id.startswith("ds_error_") else "processed"
    return {
        "message": f"Datasource for {filename} refreshed successfully",
        "datasource": datasource,
        "status": status
    }


@router.get("/{session_id}/cache/stats")
async def get_cache_stats(session_id: str):
    """Get cache statistics for debugging."""
    session = session_manager.get_session(session_id)
    if not session:
        raise HTTPException(status_code=404, detail="Session not found or expired")
    
    stats = file_parser_service.get_cache_stats(session_id)
    return {"session_id": session_id, "cache_stats": stats}


@router.delete("/{session_id}/cache")
async def clear_cache(session_id: str):
    """Clear all cached datasources for a session."""
    session = session_manager.get_session(session_id)
    if not session:
        raise HTTPException(status_code=404, detail="Session not found or expired")
    
    success = file_parser_service.clear_cache(session_id)
    if not success:
        raise HTTPException(status_code=500, detail="Failed to clear cache")
    
    return {"message": f"Cache cleared for session {session_id}"}
