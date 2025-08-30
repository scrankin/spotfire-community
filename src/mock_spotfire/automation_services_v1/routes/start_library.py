from fastapi import APIRouter, Query

from ..errors import InvalidJobDefinitionError, MissingArgumentsError
from ..state import state
from ..models import JobDefinition
from spotfire_community.automation_services.models import (
    ExecutionStatusResponse,
    ExecutionStatus,
)


router = APIRouter(prefix="/spotfire/api/rest/as")


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
    job_definition: JobDefinition | None
    if job_definition_id is None and library_path is None:
        raise InvalidJobDefinitionError()
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


__all__ = ["router"]
