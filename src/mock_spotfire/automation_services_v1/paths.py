from fastapi import (
    APIRouter,
    Query,
    Request,
)

from .errors import (
    InvalidJobIdError,
    JobNotFoundError,
    EmptyJobBodyError,
    InvalidContentType,
    InvalidJobDefinitionError,
    JobDefinitionNotFoundError,
    InvalidJobStatusError,
    MissingArgumentsError,
)
from .models import ExecutionStatusResponse, JobDefinition, ExecutionStatus
from .state import state
from .._core.uuid import is_valid_uuid


router = APIRouter()

router.prefix = "/spotfire/api/rest/as"


@router.get("/job/status/{job_id}")
def job_status(
    job_id: str,
):
    if not is_valid_uuid(job_id):
        raise InvalidJobIdError(job_id)
    job = state.get_job(job_id=job_id)
    if job is None:
        raise JobNotFoundError()
    return ExecutionStatusResponse(
        jobId=job.id, statusCode=job.status, message="placeholder"
    )


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
        jobId=job.id, statusCode=job.status, message="placeholder"
    )


@router.post("/job/start-content")
async def start_xml_job(request: Request):
    if content_type := request.headers.get("content-type") != "application/xml":
        raise InvalidContentType(
            f"Content-Type should be application/xml, received {content_type}"
        )
    body = await request.body()
    if not body.strip():
        raise EmptyJobBodyError()
    # TODO: add check for valid xml
    job = state.add_new_job()
    return ExecutionStatusResponse(
        jobId=job.id, statusCode=job.status, message="placeholder"
    )


@router.post("/job/start-library")
def start_library_job(
    job_definition_id: str | None = Query(
        alias="id", description="The library ID of the Automation Services job"
    ),
    library_path: str | None = Query(
        description="The library path of the Automation Services job"
    ),
):
    job_definition: JobDefinition | None
    if job_definition_id is None and library_path is None:
        raise InvalidJobDefinitionError()
    # Spotfire seems to prioritize id over path.
    # I tested sending path and id for two different job definitions
    # and it chose the id'd job.
    elif job_definition_id is not None:
        job_definition = state.get_job_definition_by_id(job_definition_id)
    elif library_path is not None:
        job_definition = state.get_job_definition_by_path(library_path)
    else:
        raise MissingArgumentsError()

    if job_definition is None:
        raise JobDefinitionNotFoundError()

    job = state.add_new_job()
    return ExecutionStatusResponse(
        jobId=job.id,
        statusCode=job.status,
        message="placeholder",
    )


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
