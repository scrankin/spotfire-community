from fastapi import APIRouter, Request
from xml.etree import ElementTree

from ..errors import InvalidContentType, InvalidJobDefinitionXMLError
from ..state import state
from spotfire_community.automation_services.models import ExecutionStatusResponse


router = APIRouter(prefix="/spotfire/api/rest/as")


@router.post("/job/start-content")
async def start_xml_job(request: Request):
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


__all__ = ["router"]
