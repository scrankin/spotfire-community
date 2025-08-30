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


def test_job_cancel_behavior(test_client: TestClient):
    client = AutomationServicesClient(
        spotfire_url="http://testserver",
        client_id="id",
        client_secret="secret",
    )

    # Test non-existent job id
    with pytest.raises(InvalidJobDefinitionIdError):
        client.start_library_job_definition(job_definition_id="invalid_job_id")

    with pytest.raises(JobDefinitionNotFoundError):
        client.start_library_job_definition(job_definition_id=str(uuid4()))

    with pytest.raises(JobDefinitionNotFoundError):
        client.start_library_job_definition(library_path="/non-existant")

    with pytest.raises(JobDefinitionNotFoundError):
        client.start_library_job_definition()

    job = client.start_library_job_definition(
        job_definition_id=EXISTING_JOB_DEFINITION_ID
    )

    # Check status from mock api
    assert job.status_code == ExecutionStatus.IN_PROGRESS
