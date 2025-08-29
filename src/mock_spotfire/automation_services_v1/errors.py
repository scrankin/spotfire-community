from fastapi import HTTPException


class InvalidJobIdError(HTTPException):
    def __init__(self, job_id: str):
        super().__init__(
            status_code=400, detail=f"Invalid job ID: {job_id}. Should be a UUID"
        )


class JobNotFoundError(HTTPException):
    def __init__(self):
        super().__init__(status_code=404, detail=f"Job not Found")


class EmptyJobBodyError(HTTPException):
    def __init__(self):
        super().__init__(status_code=400, detail=f"Empty job body")


class InvalidContentType(HTTPException):
    def __init__(self, message: str):
        super().__init__(status_code=415, detail=message)


class InvalidJobXMLError(HTTPException):
    def __init__(self):
        super().__init__(status_code=400, detail=f"Invalid job XML")


class InvalidJobDefinitionError(HTTPException):
    def __init__(self):
        super().__init__(status_code=400, detail=f"Invalid job definition")


class InvalidJobStatusError(HTTPException):
    def __init__(self):
        super().__init__(status_code=400, detail=f"Invalid job status")


class MissingArgumentsError(HTTPException):
    def __init__(self):
        super().__init__(status_code=400, detail=f"Missing arguments.")


__all__ = [
    "JobNotFoundError",
    "EmptyJobBodyError",
    "InvalidContentType",
    "InvalidJobXMLError",
    "InvalidJobDefinitionError",
    "InvalidJobStatusError",
    "MissingArgumentsError",
]
