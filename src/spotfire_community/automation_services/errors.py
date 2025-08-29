from typing import Optional


class JobNotFoundError(Exception):
    def __init__(self, job_id: str):
        super().__init__(f"Job with ID {job_id} not found")


class InvalidJobIdError(Exception):
    def __init__(self, job_id: str):
        super().__init__(f"Invalid job ID: {job_id}. Should be a UUID.")


class InvalidJobDefinitionIdError(Exception):
    def __init__(self, job_definition_id: str):
        super().__init__(
            f"Invalid job definition ID: {job_definition_id}. Should be a UUID."
        )


class JobDefinitionNotFoundError(Exception):
    def __init__(
        self,
        *,
        job_definition_id: Optional[str] = None,
        library_path: Optional[str] = None,
    ):
        if job_definition_id is not None:
            super().__init__(f"Job definition with ID {job_definition_id} not found")
        elif library_path is not None:
            super().__init__(
                f"Job definition with library path {library_path} not found"
            )
        else:
            super().__init__("Job definition not found, no parameters provided")


__all__ = [
    "JobNotFoundError",
    "InvalidJobIdError",
    "InvalidJobDefinitionIdError",
    "JobDefinitionNotFoundError",
]
