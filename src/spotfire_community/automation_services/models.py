"""Public models for Automation Services client responses and enums."""

from pydantic import BaseModel, ConfigDict
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


class ExecutionStatusResponse(BaseModel):
    """Response payload returned by status and start endpoints."""

    model_config = ConfigDict(
        alias_generator=lambda s: "".join(part.title() for part in s.split("_")),
        populate_by_name=True,
    )
    status_code: ExecutionStatus
    message: str
    job_id: str
