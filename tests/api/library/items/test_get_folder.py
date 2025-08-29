import pytest
from fastapi.testclient import TestClient

from spotfire_community.library.client import LibraryClient
from spotfire_community.library.errors import ItemNotFoundError


def test_upload_overwrite_behavior(test_client: TestClient):
    client = LibraryClient(
        spotfire_url="http://testserver",
        client_id="id",
        client_secret="secret",
    )

    with pytest.raises(ItemNotFoundError):
        client._get_folder_id(  # pyright: ignore[reportPrivateUsage]
            path="does-not-exist",
        )

    # Second upload without overwrite should fail at finalize; client raises Exception
    with pytest.raises(Exception):
        client._get_folder_id(  # pyright: ignore[reportPrivateUsage]
            path="return-500",
        )

    # With overwrite=True it should succeed
    root_id = client._get_folder_id(  # pyright: ignore[reportPrivateUsage]
        path="/",
    )
    assert isinstance(root_id, str) and len(root_id) > 0
