from dataclasses import dataclass
from enum import Enum


class ExecutionStatus(Enum):
    NOT_SET = "NotSet"
    QUEUED = "Queued"
    IN_PROGRESS = "InProgress"
    FINISHED = "Finished"
    FAILED = "Failed"
    MISSING = "Missing"
    BUSY = "Busy"
    CANCELED = "Canceled"


@dataclass
class ExecutionStatusResponse:
    statusCode: ExecutionStatus
    message: str
    jobId: str


@dataclass
class Job:
    id: str
    status: ExecutionStatus


@dataclass
class JobDefinition:
    id: str
    library_path: str


__all__ = [
    "ExecutionStatus",
    "ExecutionStatusResponse",
    "Job",
    "JobDefinition",
]
