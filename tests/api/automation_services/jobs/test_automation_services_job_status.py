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


def test_get_job_status_invalid_id_raises(test_client: TestClient):
    client = AutomationServicesClient(
        spotfire_url="http://testserver",
        client_id="id",
        client_secret="secret",
    )
    with pytest.raises(InvalidJobIdError):
        client.get_job_status("invalid_job_id")


def test_get_job_status_not_found_raises(test_client: TestClient):
    client = AutomationServicesClient(
        spotfire_url="http://testserver",
        client_id="id",
        client_secret="secret",
    )
    with pytest.raises(JobNotFoundError):
        client.get_job_status(str(uuid4()))


def test_get_job_status_success_returns_status(test_client: TestClient):
    client = AutomationServicesClient(
        spotfire_url="http://testserver",
        client_id="id",
        client_secret="secret",
    )
    status = client.get_job_status(EXISTING_JOB_ID)
    assert status == ExecutionStatus.QUEUED
