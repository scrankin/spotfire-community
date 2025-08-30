"""Public models for Automation Services client responses and enums."""

from dataclasses import dataclass
from enum import StrEnum


class ExecutionStatus(StrEnum):
    """Execution status values returned by Automation Services."""

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
    """Response payload returned by status and start endpoints."""

    statusCode: ExecutionStatus
    message: str
    jobId: str
