from fastapi import APIRouter, Query

from ..errors import JobNotFoundError, InvalidJobStatusError
from ..state import state
from ..models import ExecutionStatus


router = APIRouter(prefix="/spotfire/api/rest/as")


@router.post("/job/_set_job_status")
def set_job_status(
    job_id: str = Query(..., description="The ID of the job to update"),
    status: str = Query(
        ...,
        description="The new status for the job. One of NotSet, Queued, InProgress, Finished, Failed, Missing, Busy, Canceled",
    ),
):
    job = state.get_job(job_id=job_id)
    if job is None:
        raise JobNotFoundError()
    try:
        job.status = ExecutionStatus(status)
    except ValueError:
        raise InvalidJobStatusError()


__all__ = ["router"]
