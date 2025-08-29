import uuid

from .models import Job, ExecutionStatus, JobDefinition


EXISTING_JOB_ID = "598f5e27-4a62-4ecc-bb05-2a27a0f13289"
JOB_ID_TO_CANCEL = "d2c5f5e2-4a62-4ecc-bb05-2a27a0f13289"


class AutomationServicesState:
    jobs: list[Job]
    library_job_definitions: list[JobDefinition]

    def __init__(self):
        self.jobs = [
            Job(id=EXISTING_JOB_ID, status=ExecutionStatus.QUEUED),
            Job(id=JOB_ID_TO_CANCEL, status=ExecutionStatus.IN_PROGRESS),
        ]
        self.library_job_definitions = [
            JobDefinition(id="test_job_definition", library_path="/test/job_definition")
        ]

    def add_new_job(self) -> Job:
        job = Job(
            id=str(uuid.uuid4()),
            status=ExecutionStatus.IN_PROGRESS,
        )
        self.jobs.append(job)
        return job

    def get_job(self, job_id: str) -> Job | None:
        return next((job for job in self.jobs if job.id == job_id), None)

    def cancel_job(self, job: Job) -> None:
        job.status = ExecutionStatus.CANCELED

    def get_job_definition_by_id(self, job_definition_id: str) -> JobDefinition | None:
        return next(
            (jd for jd in self.library_job_definitions if jd.id == job_definition_id),
            None,
        )

    def get_job_definition_by_path(self, library_path: str) -> JobDefinition | None:
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
