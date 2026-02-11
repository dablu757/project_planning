import os
from datetime import datetime, timezone
from typing import Any

from celery import Celery, chain

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


def _mark_job_failed(job_id: str, module_name: str, exc: Exception) -> None:
    repo.mark_failed(job_id, module_name=module_name, error=str(exc))


@celery_app.task(name="tc.run_wsb")
def run_wsb(job_id: str, payload: dict[str, Any]) -> dict[str, Any]:
    dto = PipelineInputDTO(**payload)

    try:
        repo.update_job_progress(job_id, module_name="wsb", progress=10, status="running")
        output = _run_wsb_module(job_id, dto)
        repo.update_job_progress(
            job_id,
            module_name="wsb",
            progress=30,
            status="running",
            output=output,
        )
    except Exception as exc:  # noqa: BLE001
        _mark_job_failed(job_id, "wsb", exc)
        raise

    return output


@celery_app.task(name="tc.run_task_creation")
def run_task_creation(
    wsb_output: dict[str, Any],
    job_id: str,
    payload: dict[str, Any],
) -> dict[str, Any]:
    dto = PipelineInputDTO(**payload)

    try:
        output = _run_task_creation_module(job_id, wsb_output, dto)
        repo.update_job_progress(
            job_id,
            module_name="task_creation",
            progress=55,
            status="running",
            output=output,
        )
    except Exception as exc:  # noqa: BLE001
        _mark_job_failed(job_id, "task_creation", exc)
        raise

    return output


@celery_app.task(name="tc.run_task_dependency")
def run_task_dependency(
    task_output: dict[str, Any],
    job_id: str,
    payload: dict[str, Any],
) -> dict[str, Any]:
    dto = PipelineInputDTO(**payload)

    try:
        output = _run_task_dependency_module(job_id, task_output, dto)
        repo.update_job_progress(
            job_id,
            module_name="task_dependency",
            progress=80,
            status="running",
            output=output,
        )
    except Exception as exc:  # noqa: BLE001
        _mark_job_failed(job_id, "task_dependency", exc)
        raise

    return output


@celery_app.task(name="tc.run_schedule_duration")
def run_schedule_duration(
    dependency_output: dict[str, Any],
    job_id: str,
    payload: dict[str, Any],
) -> dict[str, Any]:
    dto = PipelineInputDTO(**payload)

    try:
        output = _run_schedule_duration_module(job_id, dependency_output, dto)
        repo.update_job_progress(
            job_id,
            module_name="schedule_duration",
            progress=100,
            status="completed",
            output=output,
        )
    except Exception as exc:  # noqa: BLE001
        _mark_job_failed(job_id, "schedule_duration", exc)
        raise

    return output


@celery_app.task(name="tc.finalize_pipeline")
def finalize_pipeline(_: dict[str, Any], job_id: str) -> dict[str, Any]:
    return {"job_id": job_id, "status": "completed"}


def start_pipeline(payload: dict[str, Any]) -> tuple[str, str]:
    job_id = repo.create_job(payload)

    workflow = chain(
        run_wsb.s(job_id=job_id, payload=payload),
        run_task_creation.s(job_id=job_id, payload=payload),
        run_task_dependency.s(job_id=job_id, payload=payload),
        run_schedule_duration.s(job_id=job_id, payload=payload),
        finalize_pipeline.s(job_id=job_id),
    )
    async_result = workflow.apply_async()

    return job_id, async_result.id


def fetch_job(job_id: str) -> dict[str, Any] | None:
    return repo.get_job(job_id)
