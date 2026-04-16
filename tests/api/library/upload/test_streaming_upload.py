from fastapi.testclient import TestClient

from spotfire_community.library.client import LibraryClient
from spotfire_community.library.models import ItemType


def test_streaming_upload_single_chunk(test_client: TestClient):
    """Streaming upload with a single chunk should behave like upload_file."""
    client = LibraryClient(
        spotfire_url="http://testserver",
        client_id="id",
        client_secret="secret",
    )

    file_id = client.upload_file_streaming(
        data_stream=iter([b"hello world"]),
        path="/StreamTest/single_chunk.sbdf",
        item_type=ItemType.SBDF,
        description="Single chunk streaming test",
        overwrite=False,
    )
    assert isinstance(file_id, str) and len(file_id) > 0


def test_streaming_upload_multiple_chunks(test_client: TestClient):
    """Streaming upload with multiple chunks should send each sequentially."""
    client = LibraryClient(
        spotfire_url="http://testserver",
        client_id="id",
        client_secret="secret",
    )

    chunks = [b"chunk1", b"chunk2", b"chunk3", b"chunk4"]
    file_id = client.upload_file_streaming(
        data_stream=iter(chunks),
        path="/StreamTest/multi_chunk.sbdf",
        item_type=ItemType.SBDF,
        description="Multi chunk streaming test",
        overwrite=False,
    )
    assert isinstance(file_id, str) and len(file_id) > 0


def test_streaming_upload_empty_stream_raises(test_client: TestClient):
    """Streaming upload with no data should raise ValueError."""
    client = LibraryClient(
        spotfire_url="http://testserver",
        client_id="id",
        client_secret="secret",
    )

    import pytest

    with pytest.raises(ValueError, match="no data"):
        client.upload_file_streaming(
            data_stream=iter([]),
            path="/StreamTest/empty.sbdf",
            item_type=ItemType.SBDF,
        )


def test_streaming_upload_skips_empty_chunks(test_client: TestClient):
    """Empty bytes chunks in the stream should be skipped."""
    client = LibraryClient(
        spotfire_url="http://testserver",
        client_id="id",
        client_secret="secret",
    )

    chunks = [b"", b"real_data", b"", b"more_data"]
    file_id = client.upload_file_streaming(
        data_stream=iter(chunks),
        path="/StreamTest/skip_empty.sbdf",
        item_type=ItemType.SBDF,
        overwrite=False,
    )
    assert isinstance(file_id, str) and len(file_id) > 0


def test_streaming_upload_with_overwrite(test_client: TestClient):
    """Streaming upload should support overwriting existing files."""
    client = LibraryClient(
        spotfire_url="http://testserver",
        client_id="id",
        client_secret="secret",
    )

    # First upload
    file_id_1 = client.upload_file_streaming(
        data_stream=iter([b"version1"]),
        path="/StreamTest/overwrite.sbdf",
        item_type=ItemType.SBDF,
        overwrite=False,
    )

    # Overwrite
    file_id_2 = client.upload_file_streaming(
        data_stream=iter([b"version2"]),
        path="/StreamTest/overwrite.sbdf",
        item_type=ItemType.SBDF,
        overwrite=True,
    )

    # Should reuse the same item ID on overwrite
    assert file_id_1 == file_id_2
