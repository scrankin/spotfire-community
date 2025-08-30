import pytest
from fastapi.testclient import TestClient
from xml.etree.ElementTree import Element

from spotfire_community.automation_services._xml import (
    JobDefinition,
    Task,
    OpenAnalysisTask,
    ApplyBookmarkTask,
)


def test_open_analysis_task(test_client: TestClient):
    open_analysis_task = OpenAnalysisTask(
        path="/path/to/analysis/file", configuration_block="here"
    )

    assert isinstance(open_analysis_task, Task)

    serialized = open_analysis_task.serialize()
    # TODO: Implement serialization test
    assert isinstance(serialized, Element)


def test_apply_bookmark_task(test_client: TestClient):
    with pytest.raises(Exception):
        ApplyBookmarkTask(
            bookmark_id="Test Bookmark",
            bookmark_name="Test Bookmark",
        )

    with pytest.raises(Exception):
        ApplyBookmarkTask()

    apply_bookmark_task_by_name = ApplyBookmarkTask(
        bookmark_name="Test Bookmark",
    )

    assert isinstance(apply_bookmark_task_by_name, Task)

    serialized = apply_bookmark_task_by_name.serialize()
    # TODO: Implement serialization test
    assert isinstance(serialized, Element)

    apply_bookmark_task_by_id = ApplyBookmarkTask(
        bookmark_id="Test Bookmark",
    )

    assert isinstance(apply_bookmark_task_by_id, Task)

    serialized = apply_bookmark_task_by_id.serialize()
    # TODO: Implement serialization test
    assert isinstance(serialized, Element)


def test_job_definition(test_client: TestClient):
    job_definition = JobDefinition()
    job_definition.add_task(ApplyBookmarkTask(bookmark_id="Test Bookmark"))
    job_definition.add_task(
        OpenAnalysisTask(path="/path/to/analysis/file", configuration_block="here")
    )

    assert isinstance(job_definition, JobDefinition)
    assert len(job_definition.get_tasks()) == 2

    serialized = job_definition.serialize()
    # TODO: Implement serialization test
    assert isinstance(serialized, Element)

    bytes_serialized = job_definition.as_bytes()
    assert isinstance(bytes_serialized, bytes)
