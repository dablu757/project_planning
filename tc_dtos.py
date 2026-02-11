from dataclasses import dataclass, field
from datetime import datetime
from typing import Any


@dataclass(slots=True)
class PipelineInputDTO:
    """Payload required to run the full module pipeline."""

    wsb_input: dict[str, Any]
    task_options: dict[str, Any] = field(default_factory=dict)
    dependency_options: dict[str, Any] = field(default_factory=dict)
    schedule_options: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class JobProgressDTO:
    """Tracks the current progress of a job."""

    job_id: str
    status: str
    progress: int
    updated_at: datetime
