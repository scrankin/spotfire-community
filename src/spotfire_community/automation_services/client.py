"""Client for Spotfire Automation Services REST endpoints."""

import time
from typing import Optional

from .._core.rest import SpotfireRequestsSession, authenticate, Scope
from .._core.validation import is_valid_uuid
from .errors import (
    JobNotFoundError,
    InvalidJobIdError,
    InvalidJobDefinitionIdError,
    JobDefinitionNotFoundError,
    InvalidJobDefinitionXMLError,
)
from .models import ExecutionStatusResponse, ExecutionStatus
from ._xml import JobDefinition


class AutomationServicesClient:
    """High-level client for starting and monitoring Automation Services jobs."""

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
        """Create an authenticated client using OAuth2 client credentials."""
        self._url = f"{spotfire_url.rstrip('/')}/spotfire/api/rest/as"
        self._requests_session = SpotfireRequestsSession(timeout=timeout)

        authenticate(
            requests_session=self._requests_session,
            url=f"{spotfire_url.rstrip('/')}/spotfire",
            scopes=[Scope.AUTOMATION_SERVICES_EXECUTE],
            client_id=client_id,
            client_secret=client_secret,
        )

    def _wait_for_job_status(
        self,
        job_id: str,
        target_statuses: list[ExecutionStatus],
        poll_interval: float = 1.0,
        timeout: float = 30.0,
    ) -> ExecutionStatus:
        """Wait for a job to reach a specific status."""
        start_time = time.monotonic()
        while time.monotonic() - start_time < timeout:
            status = self.get_job_status(job_id)
            if status in target_statuses:
                return status
            time.sleep(poll_interval)
        raise TimeoutError(
            f"Job {job_id} did not reach status {target_statuses} in time."
        )

    def get_job_status(
        self,
        job_id: str,
    ) -> ExecutionStatus:
        """Fetch the current status of a job by id.

        Raises InvalidJobIdError for non-UUID input and JobNotFoundError for 404.
        """
        if not is_valid_uuid(job_id):
            raise InvalidJobIdError(job_id)
        response = self._requests_session.get(f"{self._url}/job/status/{job_id}")
        if response.status_code == 404:
            raise JobNotFoundError(job_id)
        data = ExecutionStatusResponse.model_validate(response.json())
        return data.status_code

    def cancel_job(
        self,
        job_id: str,
    ) -> ExecutionStatus:
        """Cancel an in-progress job and return its resulting status."""
        if not is_valid_uuid(job_id):
            raise InvalidJobIdError(job_id)
        response = self._requests_session.post(f"{self._url}/job/abort/{job_id}")
        if response.status_code == 404:
            raise JobNotFoundError(job_id)
        data = ExecutionStatusResponse.model_validate(response.json())
        return data.status_code

    def start_library_job_definition(
        self,
        *,
        job_definition_id: Optional[str] = None,
        library_path: Optional[str] = None,
    ) -> ExecutionStatusResponse:
        """Start a job from a saved job definition by id or library path."""
        if job_definition_id is not None and not is_valid_uuid(job_definition_id):
            raise InvalidJobDefinitionIdError(job_definition_id)
        response = self._requests_session.post(
            url=f"{self._url}/job/start-library",
            params={"id": job_definition_id, "path": library_path},
        )

        data = ExecutionStatusResponse.model_validate(response.json())
        if (
            data.status_code == ExecutionStatus.FAILED
            and data.message == "Job file not found or no access."
        ):
            raise JobDefinitionNotFoundError(
                job_definition_id=job_definition_id,
                library_path=library_path,
            )
        return data

    def start_job_definition(
        self,
        job_definition: JobDefinition,
    ) -> ExecutionStatusResponse:
        """Start a job from an XML job definition object."""
        response = self._requests_session.post(
            url=f"{self._url}/job/start-content",
            data=job_definition.as_bytes(),
            headers={"Content-Type": "application/xml"},
        )
        if response.status_code == 400:
            raise InvalidJobDefinitionXMLError()
        data = ExecutionStatusResponse.model_validate(response.json())
        return data

    def start_job_definition_and_wait(
        self,
        job_definition: JobDefinition,
        *,
        poll_interval: float = 1.0,
        timeout: float = 60.0,
    ) -> ExecutionStatus:
        """Start a job and poll until it finishes, fails, or times out.

        Returns the final ExecutionStatus. Raises TimeoutError on timeout.
        """
        job = self.start_job_definition(job_definition)
        return self._wait_for_job_status(
            job_id=job.job_id,
            target_statuses=[
                ExecutionStatus.FINISHED,
                ExecutionStatus.FAILED,
                ExecutionStatus.CANCELED,
            ],
            poll_interval=poll_interval,
            timeout=timeout,
        )

    def start_library_job_definition_and_wait(
        self,
        *,
        job_definition_id: Optional[str] = None,
        library_path: Optional[str] = None,
        poll_interval: float = 1.0,
        timeout: float = 60.0,
    ) -> ExecutionStatus:
        """Start a job and poll until it finishes, fails, or times out.

        Returns the final ExecutionStatus. Raises TimeoutError on timeout.
        """
        job = self.start_library_job_definition(
            job_definition_id=job_definition_id,
            library_path=library_path,
        )
        return self._wait_for_job_status(
            job_id=job.job_id,
            target_statuses=[
                ExecutionStatus.FINISHED,
                ExecutionStatus.FAILED,
                ExecutionStatus.CANCELED,
            ],
            poll_interval=poll_interval,
            timeout=timeout,
        )
