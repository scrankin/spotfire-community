import pytest
from fastapi.testclient import TestClient

from spotfire_community.library.client import LibraryClient
from spotfire_community.library.models import ItemType
from spotfire_community.library.errors import ItemNotFoundError


def test_upload_and_delete_flow(test_client: TestClient):
    client = LibraryClient(
        spotfire_url="http://testserver",
        client_id="id",
        client_secret="secret",
    )

    # Upload file to /Samples/Doc1
    file_id = client.upload_file(
        data=b"hello world",
        path="/Samples/Doc1",
        item_type=ItemType.DXP,
        description="Test file",
        overwrite=False,
    )
    assert isinstance(file_id, str) and len(file_id) > 0

    # Deleting folder should succeed
    client.delete_folder("/Samples")

    # After delete, folder should not be found
    with pytest.raises(ItemNotFoundError) as get_folder_exception:
        client._get_folder_id("/Samples")  # pyright: ignore[reportPrivateUsage]
