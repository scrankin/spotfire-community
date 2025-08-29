from typing import Optional

from .._core.rest import SpotfireRequestsSession, authenticate, Scope
from .errors import JobNotFoundError, InvalidJobIdError
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
        response = self._requests_session.get(f"{self._url}/job/status/{job_id}")
        if response.status_code == 400:
            raise InvalidJobIdError(job_id)
        if response.status_code == 404:
            raise JobNotFoundError(job_id)
        data = ExecutionStatusResponse(**response.json())
        return data.statusCode

    def cancel_job(
        self,
        job_id: str,
    ):
        response = self._requests_session.post(f"{self._url}/job/abort/{job_id}")
        if response.status_code == 400:
            raise InvalidJobIdError(job_id)
        if response.status_code == 404:
            raise JobNotFoundError(job_id)
        data = ExecutionStatusResponse(**response.json())
        return data.statusCode
