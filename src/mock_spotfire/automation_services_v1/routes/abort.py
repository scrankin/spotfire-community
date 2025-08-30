from fastapi import APIRouter, Query

from ..errors import InvalidJobIdError, JobNotFoundError
from ..state import state
from spotfire_community._core.validation import is_valid_uuid
from spotfire_community.automation_services.models import ExecutionStatusResponse


router = APIRouter(prefix="/spotfire/api/rest/as")


@router.post("/job/abort/{job_id}")
def cancel_job(
    job_id: str,
    reason: str | None = Query(
        default=None, description="A text describing the reason for aborting the job"
    ),
):
    if not is_valid_uuid(job_id):
        raise InvalidJobIdError(job_id)
    job = state.get_job(job_id=job_id)
    if job is None:
        raise JobNotFoundError()
    state.cancel_job(job)
    return ExecutionStatusResponse(
        job_id=job.id,
        status_code=job.status,
        message="placeholder",
    ).model_dump(by_alias=True)


__all__ = ["router"]
