import pytest
from fastapi.testclient import TestClient
from uuid import uuid4

from spotfire_community.automation_services import AutomationServicesClient
from spotfire_community.automation_services.errors import (
    JobNotFoundError,
    InvalidJobIdError,
)
from spotfire_community.automation_services.models import ExecutionStatus


EXISTING_JOB_ID = "598f5e27-4a62-4ecc-bb05-2a27a0f13289"


def test_job_status_behavior(test_client: TestClient):
    client = AutomationServicesClient(
        spotfire_url="http://testserver",
        client_id="id",
        client_secret="secret",
    )

    # Test non-existent job id
    with pytest.raises(InvalidJobIdError):
        client.get_job_status("invalid_job_id")

    with pytest.raises(JobNotFoundError):
        client.get_job_status(str(uuid4()))

    status = client.get_job_status(EXISTING_JOB_ID)

    # Check status from mock api
    assert status == ExecutionStatus.QUEUED
