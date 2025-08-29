from dataclasses import dataclass
from enum import StrEnum


class ExecutionStatus(StrEnum):
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
