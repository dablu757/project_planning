import os
from datetime import datetime, timezone
from typing import Any

from pymongo import MongoClient, ReturnDocument


class TaskChainRepository:
    """MongoDB persistence for jobs and module outputs."""

    def __init__(self, mongo_uri: str | None = None, db_name: str | None = None) -> None:
        self.mongo_uri = mongo_uri or os.getenv("MONGO_URI", "mongodb://localhost:27017")
        self.db_name = db_name or os.getenv("MONGO_DB", "project_planning")
        self.client = MongoClient(self.mongo_uri)
        self.db = self.client[self.db_name]
        self.jobs = self.db["jobs"]

    @staticmethod
    def _utcnow() -> datetime:
        return datetime.now(timezone.utc)

    def create_job(self, payload: dict[str, Any]) -> str:
        now = self._utcnow()
        result = self.jobs.insert_one(
            {
                "status": "queued",
                "progress": 0,
                "current_module": None,
                "request": payload,
                "outputs": {},
                "error": None,
                "created_at": now,
                "updated_at": now,
            }
        )
        return str(result.inserted_id)

    def update_job_progress(
        self,
        job_id: str,
        *,
        module_name: str,
        progress: int,
        status: str,
        output: dict[str, Any] | None = None,
    ) -> dict[str, Any] | None:
        update_ops: dict[str, Any] = {
            "status": status,
            "progress": progress,
            "current_module": module_name,
            "updated_at": self._utcnow(),
        }
        if output is not None:
            update_ops[f"outputs.{module_name}"] = output
            self.db[f"{module_name}_outputs"].insert_one(
                {
                    "job_id": job_id,
                    "module": module_name,
                    "output": output,
                    "created_at": self._utcnow(),
                }
            )

        return self.jobs.find_one_and_update(
            {"_id": self._to_object_id(job_id)},
            {"$set": update_ops},
            return_document=ReturnDocument.AFTER,
        )

    def mark_failed(self, job_id: str, module_name: str, error: str) -> None:
        self.jobs.update_one(
            {"_id": self._to_object_id(job_id)},
            {
                "$set": {
                    "status": "failed",
                    "current_module": module_name,
                    "error": error,
                    "updated_at": self._utcnow(),
                }
            },
        )

    def get_job(self, job_id: str) -> dict[str, Any] | None:
        return self.jobs.find_one({"_id": self._to_object_id(job_id)})

    @staticmethod
    def _to_object_id(job_id: str):
        from bson import ObjectId

        return ObjectId(job_id)
