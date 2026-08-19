"""Celery application for background upload preprocessing."""

from celery import Celery

from ..config import settings

celery_app = Celery(
    "atomspace",
    broker=settings.celery_broker_url,
    backend=settings.celery_result_backend,
    include=["app.tasks.upload_tasks"],
)

celery_app.conf.update(
    task_serializer="json",
    result_serializer="json",
    accept_content=["json"],
    timezone="UTC",
    enable_utc=True,
    task_track_started=True,
    broker_connection_retry_on_startup=True,
    task_time_limit=settings.celery_task_time_limit,
    task_soft_time_limit=settings.celery_task_soft_time_limit,
    # At-least-once delivery: a worker that crashes mid-task redelivers the
    # message. Re-runs are safe because process_single_file is idempotent
    # (terminal-state skip) and generation-token guarded.
    task_acks_late=True,
    task_acks_on_failure_or_timeout=False,
)
