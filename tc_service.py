import os
from datetime import datetime, timezone
from typing import Any

from celery import Celery

from tc_dtos import PipelineInputDTO
from tc_repo import TaskChainRepository

BROKER_URL = os.getenv("CELERY_BROKER_URL", "redis://localhost:6379/0")
RESULT_BACKEND = os.getenv("CELERY_RESULT_BACKEND", BROKER_URL)

celery_app = Celery("project_planning", broker=BROKER_URL, backend=RESULT_BACKEND)
celery_app.conf.update(
    task_serializer="json",
    result_serializer="json",
    accept_content=["json"],
    task_track_started=True,
    task_acks_late=True,
    worker_prefetch_multiplier=1,
)

repo = TaskChainRepository()


def _utcnow_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _run_wsb_module(job_id: str, dto: PipelineInputDTO) -> dict[str, Any]:
    return {
        "wsb_id": f"WSB-{job_id[-6:]}",
        "received": dto.wsb_input,
        "created_at": _utcnow_iso(),
    }


def _run_task_creation_module(
    job_id: str,
    wsb_output: dict[str, Any],
    dto: PipelineInputDTO,
) -> dict[str, Any]:
    return {
        "task_bundle_id": f"TASK-{job_id[-6:]}",
        "wsb_id": wsb_output["wsb_id"],
        "task_options": dto.task_options,
        "tasks_created": dto.task_options.get("tasks", []),
        "created_at": _utcnow_iso(),
    }


def _run_task_dependency_module(
    job_id: str,
    task_output: dict[str, Any],
    dto: PipelineInputDTO,
) -> dict[str, Any]:
    return {
        "dependency_graph_id": f"DEP-{job_id[-6:]}",
        "task_bundle_id": task_output["task_bundle_id"],
        "dependency_options": dto.dependency_options,
        "created_at": _utcnow_iso(),
    }


def _run_schedule_duration_module(
    job_id: str,
    dependency_output: dict[str, Any],
    dto: PipelineInputDTO,
) -> dict[str, Any]:
    return {
        "schedule_id": f"SCH-{job_id[-6:]}",
        "dependency_graph_id": dependency_output["dependency_graph_id"],
        "schedule_options": dto.schedule_options,
        "created_at": _utcnow_iso(),
    }


@celery_app.task(bind=True, name="tc.run_pipeline")
def run_pipeline(self, job_id: str, payload: dict[str, Any]) -> dict[str, Any]:
    dto = PipelineInputDTO(**payload)

    try:
        repo.update_job_progress(job_id, module_name="wsb", progress=10, status="running")
        wsb_output = _run_wsb_module(job_id, dto)
        repo.update_job_progress(
            job_id,
            module_name="wsb",
            progress=30,
            status="running",
            output=wsb_output,
        )

        task_output = _run_task_creation_module(job_id, wsb_output, dto)
        repo.update_job_progress(
            job_id,
            module_name="task_creation",
            progress=55,
            status="running",
            output=task_output,
        )

        dependency_output = _run_task_dependency_module(job_id, task_output, dto)
        repo.update_job_progress(
            job_id,
            module_name="task_dependency",
            progress=80,
            status="running",
            output=dependency_output,
        )

        schedule_output = _run_schedule_duration_module(job_id, dependency_output, dto)
        repo.update_job_progress(
            job_id,
            module_name="schedule_duration",
            progress=100,
            status="completed",
            output=schedule_output,
        )

    except Exception as exc:  # noqa: BLE001
        repo.mark_failed(job_id, module_name="pipeline", error=str(exc))
        raise

    return {
        "job_id": job_id,
        "status": "completed",
    }


def start_pipeline(payload: dict[str, Any]) -> tuple[str, str]:
    job_id = repo.create_job(payload)
    async_result = run_pipeline.delay(job_id=job_id, payload=payload)
    return job_id, async_result.id


def fetch_job(job_id: str) -> dict[str, Any] | None:
    return repo.get_job(job_id)
