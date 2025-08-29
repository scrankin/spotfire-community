import pytest
from fastapi.testclient import TestClient
from uuid import uuid4

from spotfire_community.automation_services import AutomationServicesClient
from spotfire_community.automation_services.errors import (
    JobNotFoundError,
    InvalidJobIdError,
)
from spotfire_community.automation_services.models import ExecutionStatus


JOB_ID_TO_CANCEL = "d2c5f5e2-4a62-4ecc-bb05-2a27a0f13289"


def test_job_cancel_behavior(test_client: TestClient):
    client = AutomationServicesClient(
        spotfire_url="http://testserver",
        client_id="id",
        client_secret="secret",
    )

    # Test non-existent job id
    with pytest.raises(InvalidJobIdError):
        client.cancel_job("invalid_job_id")

    with pytest.raises(JobNotFoundError):
        client.cancel_job(str(uuid4()))

    status = client.cancel_job(JOB_ID_TO_CANCEL)

    # Check status from mock api
    assert status == ExecutionStatus.CANCELED
