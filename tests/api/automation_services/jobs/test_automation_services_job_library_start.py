import pytest
from fastapi.testclient import TestClient
from uuid import uuid4

from mock_spotfire.automation_services_v1.state import EXISTING_JOB_DEFINITION_ID
from spotfire_community.automation_services import AutomationServicesClient
from spotfire_community.automation_services.errors import (
    JobDefinitionNotFoundError,
    InvalidJobDefinitionIdError,
)
from spotfire_community.automation_services.models import ExecutionStatus


def test_start_library_invalid_id_format_raises(test_client: TestClient):
    client = AutomationServicesClient(
        spotfire_url="http://testserver",
        client_id="id",
        client_secret="secret",
    )
    with pytest.raises(InvalidJobDefinitionIdError):
        client.start_library_job_definition(job_definition_id="invalid_job_id")


def test_start_library_unknown_id_raises(test_client: TestClient):
    client = AutomationServicesClient(
        spotfire_url="http://testserver",
        client_id="id",
        client_secret="secret",
    )
    with pytest.raises(JobDefinitionNotFoundError):
        client.start_library_job_definition(job_definition_id=str(uuid4()))


def test_start_library_unknown_path_raises(test_client: TestClient):
    client = AutomationServicesClient(
        spotfire_url="http://testserver",
        client_id="id",
        client_secret="secret",
    )
    with pytest.raises(JobDefinitionNotFoundError):
        client.start_library_job_definition(library_path="/non-existant")


def test_start_library_missing_args_raises(test_client: TestClient):
    client = AutomationServicesClient(
        spotfire_url="http://testserver",
        client_id="id",
        client_secret="secret",
    )
    with pytest.raises(JobDefinitionNotFoundError):
        client.start_library_job_definition()


def test_start_library_success_returns_in_progress(test_client: TestClient):
    client = AutomationServicesClient(
        spotfire_url="http://testserver",
        client_id="id",
        client_secret="secret",
    )
    job = client.start_library_job_definition(
        job_definition_id=EXISTING_JOB_DEFINITION_ID
    )
    assert job.status_code == ExecutionStatus.IN_PROGRESS
