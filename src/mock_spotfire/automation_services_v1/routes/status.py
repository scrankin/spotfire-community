from fastapi import APIRouter
import time

from ..errors import InvalidJobIdError, JobNotFoundError
from ..state import state
from ..models import ExecutionStatus
from spotfire_community._core.validation import is_valid_uuid
from spotfire_community.automation_services.models import ExecutionStatusResponse


router = APIRouter(prefix="/spotfire/api/rest/as")


@router.get("/job/status/{job_id}")
def job_status(job_id: str):
    if not is_valid_uuid(job_id):
        raise InvalidJobIdError(job_id)
    job = state.get_job(job_id=job_id)
    if job is None:
        raise JobNotFoundError()
    # If job is IN_PROGRESS and 1s have passed, mark as FINISHED
    if job.status == ExecutionStatus.IN_PROGRESS:
        if time.monotonic() - job.created_at > 1:
            job.status = ExecutionStatus.FINISHED
    return ExecutionStatusResponse(
        job_id=job.id,
        status_code=job.status,
        message="placeholder",
    ).model_dump(by_alias=True)


__all__ = ["router"]
