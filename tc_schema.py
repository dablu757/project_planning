from datetime import datetime
from typing import Any

from pydantic import BaseModel, Field


class PipelineRequest(BaseModel):
    wsb_input: dict[str, Any] = Field(..., description="Raw WSB creation input from frontend")
    task_options: dict[str, Any] = Field(default_factory=dict)
    dependency_options: dict[str, Any] = Field(default_factory=dict)
    schedule_options: dict[str, Any] = Field(default_factory=dict)


class PipelineStartResponse(BaseModel):
    job_id: str
    celery_task_id: str
    status: str


class JobStatusResponse(BaseModel):
    job_id: str
    status: str
    progress: int
    current_module: str | None = None
    outputs: dict[str, Any] = Field(default_factory=dict)
    created_at: datetime
    updated_at: datetime
    error: str | None = None
