from fastapi import APIRouter, HTTPException, status

from tc_schema import JobStatusResponse, PipelineRequest, PipelineStartResponse
from tc_service import fetch_job, start_pipeline

router = APIRouter(prefix="/api/v1/tc", tags=["task-chain"])


@router.post("/jobs", response_model=PipelineStartResponse, status_code=status.HTTP_202_ACCEPTED)
def create_job(request: PipelineRequest) -> PipelineStartResponse:
    job_id, celery_task_id = start_pipeline(request.model_dump())
    return PipelineStartResponse(job_id=job_id, celery_task_id=celery_task_id, status="queued")


@router.get("/jobs/{job_id}", response_model=JobStatusResponse)
def get_job_status(job_id: str) -> JobStatusResponse:
    job_doc = fetch_job(job_id)
    if not job_doc:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Job not found")

    return JobStatusResponse(
        job_id=str(job_doc["_id"]),
        status=job_doc["status"],
        progress=job_doc["progress"],
        current_module=job_doc.get("current_module"),
        outputs=job_doc.get("outputs", {}),
        created_at=job_doc["created_at"],
        updated_at=job_doc["updated_at"],
        error=job_doc.get("error"),
    )
