from fastapi import HTTPException


class InvalidJobIdError(HTTPException):
    """400: Invalid job id format (expects UUID)."""

    def __init__(self, job_id: str):
        super().__init__(
            status_code=400, detail=f"Invalid job ID: {job_id}. Should be a UUID"
        )


class JobNotFoundError(HTTPException):
    """404: Job not found in in-memory state."""

    def __init__(self):
        super().__init__(status_code=404, detail=f"Job not Found")


class InvalidJobDefinitionXMLError(HTTPException):
    """400: Job definition XML was invalid or missing."""

    def __init__(self):
        super().__init__(status_code=400, detail=f"Invalid job definition XML")


class InvalidContentType(HTTPException):
    """415: Content-Type is not supported for the endpoint."""

    def __init__(self, message: str):
        super().__init__(status_code=415, detail=message)


class InvalidJobXMLError(HTTPException):
    """400: Job XML was invalid."""

    def __init__(self):
        super().__init__(status_code=400, detail=f"Invalid job XML")


class InvalidJobDefinitionError(HTTPException):
    """400: Required parameters for job definition were not provided."""

    def __init__(self):
        super().__init__(status_code=400, detail=f"Invalid job definition")


class InvalidJobStatusError(HTTPException):
    """400: Provided status string is not a valid ExecutionStatus."""

    def __init__(self):
        super().__init__(status_code=400, detail=f"Invalid job status")


class MissingArgumentsError(HTTPException):
    """400: Expected arguments were not provided."""

    def __init__(self):
        super().__init__(status_code=400, detail=f"Missing arguments.")


__all__ = [
    "JobNotFoundError",
    "InvalidJobDefinitionXMLError",
    "InvalidContentType",
    "InvalidJobXMLError",
    "InvalidJobDefinitionError",
    "InvalidJobStatusError",
    "MissingArgumentsError",
]
