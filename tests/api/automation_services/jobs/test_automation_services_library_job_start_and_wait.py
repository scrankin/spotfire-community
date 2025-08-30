import pytest
from fastapi.testclient import TestClient

from mock_spotfire.automation_services_v1.state import EXISTING_JOB_DEFINITION_ID
from spotfire_community.automation_services import (
    AutomationServicesClient,
)
from spotfire_community.automation_services.models import ExecutionStatus


def test_start_library_job_definition_and_wait(test_client: TestClient):
    client = AutomationServicesClient(
        spotfire_url="http://testserver",
        client_id="dummy",
        client_secret="dummy",
    )

    with pytest.raises(TimeoutError):
        client.start_library_job_definition_and_wait(
            job_definition_id=EXISTING_JOB_DEFINITION_ID, poll_interval=0.1, timeout=0.5
        )

    status = client.start_library_job_definition_and_wait(
        job_definition_id=EXISTING_JOB_DEFINITION_ID,
        library_path="dummy_path",
        poll_interval=0.1,
        timeout=2,
    )
    assert status == ExecutionStatus.FINISHED
