import pytest
from fastapi.testclient import TestClient

from spotfire_community.library.client import LibraryClient
from spotfire_community.library.models import ItemType


def test_upload_overwrite_behavior(test_client: TestClient):
    client = LibraryClient(
        spotfire_url="http://testserver",
        client_id="id",
        client_secret="secret",
    )

    # First upload
    client.upload_file(
        data=b"v1",
        path="/Reports/R1",
        item_type=ItemType.DXP,
        overwrite=False,
    )

    # Second upload without overwrite should fail at finalize; client raises Exception
    with pytest.raises(Exception):
        client.upload_file(
            data=b"v2",
            path="/Reports/R1",
            item_type=ItemType.DXP,
            overwrite=False,
        )

    # With overwrite=True it should succeed
    second_id = client.upload_file(
        data=b"v2",
        path="/Reports/R1",
        item_type=ItemType.DXP,
        overwrite=True,
    )
    assert isinstance(second_id, str) and len(second_id) > 0
