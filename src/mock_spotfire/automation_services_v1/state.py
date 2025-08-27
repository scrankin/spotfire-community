import uuid

from .models import Job, ExecutionStatus, JobDefinition


class AutomationServicesState:
    running_jobs: list[Job]
    library_job_definitions: list[JobDefinition]

    def __init__(self):
        self.running_jobs = []
        self.library_job_definitions = [
            JobDefinition(id="test_job", library_path="/test/job_definition")
        ]

    def add_new_job(self) -> Job:
        job = Job(
            id=str(uuid.uuid4()),
            status=ExecutionStatus.IN_PROGRESS,
        )
        self.running_jobs.append(job)
        return job

    def get_job(self, job_id: str) -> Job | None:
        return next((job for job in self.running_jobs if job.id == job_id), None)

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
