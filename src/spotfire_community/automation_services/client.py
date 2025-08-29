from typing import Optional

from .._core.rest import SpotfireRequestsSession, authenticate, Scope
from .._core.validation import is_valid_uuid
from .errors import (
    JobNotFoundError,
    InvalidJobIdError,
    InvalidJobDefinitionIdError,
    JobDefinitionNotFoundError,
)
from .models import ExecutionStatusResponse, ExecutionStatus


class AutomationServicesClient:
    _url: str
    _requests_session: SpotfireRequestsSession

    def __init__(
        self,
        spotfire_url: str,
        client_id: str,
        client_secret: str,
        *,
        timeout: Optional[float] = 30.0,
    ):
        self._url = f"{spotfire_url.rstrip('/')}/spotfire/api/rest/as"
        self._requests_session = SpotfireRequestsSession(timeout=timeout)

        authenticate(
            requests_session=self._requests_session,
            url=f"{spotfire_url.rstrip('/')}/spotfire",
            scopes=[Scope.AUTOMATION_SERVICES_EXECUTE],
            client_id=client_id,
            client_secret=client_secret,
        )

    def _start_job_with_definition(self): ...

    def _start_job_from_library_path(self): ...

    def get_job_status(
        self,
        job_id: str,
    ) -> ExecutionStatus:
        if not is_valid_uuid(job_id):
            raise InvalidJobIdError(job_id)
        response = self._requests_session.get(f"{self._url}/job/status/{job_id}")
        if response.status_code == 404:
            raise JobNotFoundError(job_id)
        data = ExecutionStatusResponse(**response.json())
        return data.statusCode

    def cancel_job(
        self,
        job_id: str,
    ):
        if not is_valid_uuid(job_id):
            raise InvalidJobIdError(job_id)
        response = self._requests_session.post(f"{self._url}/job/abort/{job_id}")
        if response.status_code == 404:
            raise JobNotFoundError(job_id)
        data = ExecutionStatusResponse(**response.json())
        return data.statusCode

    def start_library_job_definition(
        self,
        *,
        job_definition_id: Optional[str] = None,
        library_path: Optional[str] = None,
    ):
        if job_definition_id is not None and not is_valid_uuid(job_definition_id):
            raise InvalidJobDefinitionIdError(job_definition_id)
        response = self._requests_session.post(
            url=f"{self._url}/job/start-library",
            params={"id": job_definition_id, "path": library_path},
        )

        data = ExecutionStatusResponse(**response.json())
        if (
            data.statusCode == ExecutionStatus.FAILED
            and data.message == "Job file not found or no access."
        ):
            raise JobDefinitionNotFoundError(
                job_definition_id=job_definition_id,
                library_path=library_path,
            )
        return data.statusCode
