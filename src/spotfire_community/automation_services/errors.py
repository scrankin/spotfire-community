"""Exceptions raised by the Automation Services client API."""

from typing import Optional


class JobNotFoundError(Exception):
    """Raised when a referenced job id does not exist on the server."""

    def __init__(self, job_id: str):
        super().__init__(f"Job with ID {job_id} not found")


class InvalidJobIdError(Exception):
    """Raised when a job id parameter is not a valid UUID string."""

    def __init__(self, job_id: str):
        super().__init__(f"Invalid job ID: {job_id}. Should be a UUID.")


class InvalidJobDefinitionIdError(Exception):
    """Raised when a job definition id parameter is not a valid UUID."""

    def __init__(self, job_definition_id: str):
        super().__init__(
            f"Invalid job definition ID: {job_definition_id}. Should be a UUID."
        )


class JobDefinitionNotFoundError(Exception):
    """Raised when a job definition cannot be found by id or path."""

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


class InvalidJobDefinitionXMLError(Exception):
    """Raised when an XML job definition is rejected by the server."""

    def __init__(self):
        super().__init__("Invalid job definition XML")


__all__ = [
    "JobNotFoundError",
    "InvalidJobIdError",
    "InvalidJobDefinitionIdError",
    "JobDefinitionNotFoundError",
    "InvalidJobDefinitionXMLError",
]
