import pytest
from fastapi.testclient import TestClient

from spotfire_community.automation_services import (
    AutomationServicesClient,
    JobDefinition,
    OpenAnalysisTask,
)
from spotfire_community.automation_services.errors import InvalidJobDefinitionXMLError
from spotfire_community.automation_services.models import ExecutionStatus


def test_job_start_behavior(test_client: TestClient):
    client = AutomationServicesClient(
        spotfire_url="http://testserver",
        client_id="id",
        client_secret="secret",
    )

    job_definition = JobDefinition()
    job_definition.add_task(OpenAnalysisTask(path="/test/"))

    job = client.start_job_definition(job_definition=job_definition)

    # Check status from mock api
    assert job.status_code == ExecutionStatus.IN_PROGRESS


def test_invalid_job_start_behavior(test_client: TestClient):
    client = AutomationServicesClient(
        spotfire_url="http://testserver",
        client_id="id",
        client_secret="secret",
    )

    job_definition = JobDefinition()
    job_definition.add_task(OpenAnalysisTask(path="return-invalid"))

    with pytest.raises(InvalidJobDefinitionXMLError):
        client.start_job_definition(job_definition=job_definition)
