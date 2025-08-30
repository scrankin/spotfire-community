import pytest
from fastapi.testclient import TestClient

from spotfire_community.library.client import LibraryClient


def test_upload_overwrite_behavior(test_client: TestClient):
    with pytest.raises(Exception):
        LibraryClient(
            spotfire_url="http://testserver",
            client_id="return-500",
            client_secret="return-500",
        )

    with pytest.raises(Exception):
        LibraryClient(
            spotfire_url="http://testserver",
            client_id="return-202",
            client_secret="return-202",
        )
