# Project Planning Pipeline (FastAPI + Celery + MongoDB)

This service implements a 4-step execution flow:

1. WSB creation
2. Task creation
3. Task dependency
4. Schedule duration

Execution is decoupled using Celery workers so multiple job requests can run concurrently, and each module can be routed/scaled independently.

## Features

- `POST /api/v1/tc/jobs` accepts frontend WSB input and creates a job.
- Input payload is stored in MongoDB `jobs` collection.
- Celery executes each module as an independent task in a chained sequence (`1 -> 2 -> 3 -> 4`).
- Module outputs are inserted into MongoDB (`wsb_outputs`, `task_creation_outputs`, `task_dependency_outputs`, `schedule_duration_outputs`).
- Job progress is tracked in `jobs.progress`.
  - WSB completion explicitly updates progress to **30%**.
- `GET /api/v1/tc/jobs/{job_id}` returns status/progress and outputs.

## Files

- `__init__.py`
- `tc_dtos.py`
- `tc_repo.py`
- `tc_router.py`
- `tc_schema.py`
- `tc_service.py`

## Environment variables

- `MONGO_URI` (default: `mongodb://localhost:27017`)
- `MONGO_DB` (default: `project_planning`)
- `CELERY_BROKER_URL` (default: `redis://localhost:6379/0`)
- `CELERY_RESULT_BACKEND` (default: broker URL)

## Run

```bash
pip install -r requirements.txt
uvicorn main:app --reload --port 8000
```

Run Celery worker(s) in separate terminal(s):

```bash
# Single worker process
celery -A tc_service.celery_app worker --loglevel=info

# Scale out with 4 worker processes in one command
celery -A tc_service.celery_app worker --loglevel=info --concurrency=4
```

You can also run multiple worker instances (for example 3-4 terminals/containers), all connected to the same Redis broker, and jobs will be distributed across them automatically.

## Scaling & further decoupling ideas

- Route tasks to dedicated Celery queues (`queue="wsb"`, `"dependency"`, etc.) and run specialized worker pools per module.
- Use autoscaling worker deployments (Kubernetes HPA, ECS scaling, etc.) based on queue depth.
- Add retry/backoff per task and idempotency keys to safely recover from transient errors.
- Move module outputs to an event stream (Kafka/Redis Streams) if downstream services need real-time subscriptions.
