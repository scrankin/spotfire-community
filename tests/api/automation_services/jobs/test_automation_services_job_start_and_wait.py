import pytest
from fastapi.testclient import TestClient

from spotfire_community.automation_services import (
    AutomationServicesClient,
    JobDefinition,
)
from spotfire_community.automation_services.models import ExecutionStatus


def test_start_job_definition_and_wait(test_client: TestClient):
    client = AutomationServicesClient(
        spotfire_url="http://testserver",
        client_id="dummy",
        client_secret="dummy",
    )
    job_definition = JobDefinition()

    with pytest.raises(TimeoutError):
        client.start_job_definition_and_wait(
            job_definition=job_definition, poll_interval=0.5, timeout=1
        )

    status = client.start_job_definition_and_wait(
        job_definition=job_definition, poll_interval=0.5, timeout=10
    )
    assert status == ExecutionStatus.FINISHED
