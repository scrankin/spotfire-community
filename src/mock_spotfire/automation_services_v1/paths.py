"""Mock Automation Services v1 routes for testing clients.

Endpoints here simulate a subset of the Spotfire Automation Services REST API
for use in tests. State is kept in-memory via ``state``.
"""

from fastapi import (
    APIRouter,
    Query,
    Request,
)
import time
from xml.etree import ElementTree

from .errors import (
    InvalidJobIdError,
    JobNotFoundError,
    InvalidJobDefinitionXMLError,
    InvalidContentType,
    InvalidJobDefinitionError,
    InvalidJobStatusError,
    MissingArgumentsError,
)
from .models import JobDefinition, ExecutionStatus
from .state import state
from spotfire_community._core.validation import is_valid_uuid
from spotfire_community.automation_services.models import (
    ExecutionStatusResponse,
    ExecutionStatus,
)


router = APIRouter()

router.prefix = "/spotfire/api/rest/as"


@router.get("/job/status/{job_id}")
def job_status(
    job_id: str,
):
    """Return the execution status of a job.

    Args:
        job_id: Job identifier (UUID required).

    Raises:
        InvalidJobIdError: If ``job_id`` is not a UUID.
        JobNotFoundError: If no job exists with the given id.
    """
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


@router.post("/job/abort/{job_id}")
def cancel_job(
    job_id: str,
    reason: str | None = Query(
        default=None, description="A text describing the reason for aborting the job"
    ),
):
    """Cancel a job by id and return the new status."""
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


@router.post("/job/start-content")
async def start_xml_job(request: Request):
    """Start a job from an XML job definition posted as body.

    Expects ``Content-Type: application/xml`` and a minimally valid XML body.
    The mock also accepts a body containing the marker ``return-invalid`` to
    simulate a 400 response from bad XML.
    """
    if (content_type := request.headers.get("content-type")) != "application/xml":
        raise InvalidContentType(
            f"Content-Type should be application/xml, received {content_type}"
        )
    body = await request.body()
    if len(body.strip()) == 0:
        raise InvalidJobDefinitionXMLError()
    try:
        ElementTree.fromstring(body.decode("utf-8"))
        if "return-invalid" in body.decode("utf-8"):
            raise InvalidJobDefinitionXMLError()
    except ElementTree.ParseError:
        raise InvalidJobDefinitionXMLError()
    job = state.add_new_job()
    return ExecutionStatusResponse(
        job_id=job.id,
        status_code=job.status,
        message="placeholder",
    ).model_dump(by_alias=True)


@router.post("/job/start-library")
def start_library_job(
    job_definition_id: str | None = Query(
        alias="id", description="The library ID of the Automation Services job"
    ),
    library_path: str | None = Query(
        alias="path",
        description="The library path of the Automation Services job",
    ),
):
    """Start a job from an existing library job definition.

    If both ``id`` and ``path`` are provided, id takes precedence (mirrors
    observed Spotfire behavior).
    """
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
        return ExecutionStatusResponse(
            job_id="00000000-0000-0000-0000-000000000000",
            status_code=ExecutionStatus.FAILED,
            message="Job file not found or no access.",
        ).model_dump(by_alias=True)

    job = state.add_new_job()
    return ExecutionStatusResponse(
        job_id=job.id,
        status_code=job.status,
        message="placeholder",
    ).model_dump(by_alias=True)


@router.post("/job/_set_job_status")
def set_job_status(
    job_id: str = Query(..., description="The ID of the job to update"),
    status: str = Query(
        ...,
        description="The new status for the job. One of NotSet, Queued, InProgress, Finished, Failed, Missing, Busy, Canceled",
    ),
):
    """Test hook to mutate in-memory job status."""
    job = state.get_job(job_id=job_id)
    if job is None:
        raise JobNotFoundError()
    try:
        job.status = ExecutionStatus(status)
    except ValueError:
        raise InvalidJobStatusError()


__all__ = ["router"]
