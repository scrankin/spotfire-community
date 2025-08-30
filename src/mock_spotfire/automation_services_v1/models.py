from dataclasses import dataclass, field
import time
from enum import Enum


class ExecutionStatus(Enum):
    """Execution status values used by Spotfire Automation Services."""

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
    """Response wrapper returned by status/creation endpoints."""

    statusCode: ExecutionStatus
    message: str
    jobId: str


@dataclass
class Job:
    """Represents an in-memory job in the mock service."""

    id: str
    status: ExecutionStatus
    created_at: float = field(default_factory=time.monotonic)


@dataclass
class JobDefinition:
    """Represents a library Automation Services job definition entry."""

    id: str
    library_path: str


__all__ = [
    "ExecutionStatus",
    "ExecutionStatusResponse",
    "Job",
    "JobDefinition",
]
