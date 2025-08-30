"""In-memory state store backing mock Automation Services endpoints."""

import uuid

from .models import Job, ExecutionStatus, JobDefinition


EXISTING_JOB_ID = "598f5e27-4a62-4ecc-bb05-2a27a0f13289"
JOB_ID_TO_CANCEL = "d2c5f5e2-4a62-4ecc-bb05-2a27a0f13289"
EXISTING_JOB_DEFINITION_ID = "4ef5354f-5e6b-48ea-b4b7-1e527466df9b"


class AutomationServicesState:
    """Holds jobs and job definitions for the mock Automation Services API."""

    jobs: list[Job]
    library_job_definitions: list[JobDefinition]

    def __init__(self):
        self.jobs = [
            Job(id=EXISTING_JOB_ID, status=ExecutionStatus.QUEUED),
            Job(id=JOB_ID_TO_CANCEL, status=ExecutionStatus.IN_PROGRESS),
        ]
        self.library_job_definitions = [
            JobDefinition(
                id=EXISTING_JOB_DEFINITION_ID, library_path="/test/job_definition"
            )
        ]

    def add_new_job(self) -> Job:
        """Create and register a new in-progress job."""
        job = Job(
            id=str(uuid.uuid4()),
            status=ExecutionStatus.IN_PROGRESS,
        )
        self.jobs.append(job)
        return job

    def get_job(self, job_id: str) -> Job | None:
        """Return a job by id if present."""
        return next((job for job in self.jobs if job.id == job_id), None)

    def cancel_job(self, job: Job) -> None:
        """Mark a job as canceled."""
        job.status = ExecutionStatus.CANCELED

    def get_job_definition_by_id(self, job_definition_id: str) -> JobDefinition | None:
        """Return a job definition by UUID if present."""
        return next(
            (jd for jd in self.library_job_definitions if jd.id == job_definition_id),
            None,
        )

    def get_job_definition_by_path(self, library_path: str) -> JobDefinition | None:
        """Return a job definition matching a library path if present."""
        return next(
            (
                jd
                for jd in self.library_job_definitions
                if jd.library_path == library_path
            ),
            None,
        )


# Singleton state used by handlers
state = AutomationServicesState()


__all__ = ["AutomationServicesState", "state"]
