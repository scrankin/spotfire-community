from typing import Any

import pytest

from spotfire_community.library.client import LibraryClient
from spotfire_community.library.errors import ItemNotFoundError
from spotfire_community.library.models import ItemType


class FakeResponse:
    def __init__(
        self, status_code: int, payload: dict[str, Any] | None = None, text: str = ""
    ):
        self.status_code = status_code
        self._payload = payload or {}
        self.text = text

    def json(self):  # type: ignore[override]
        return self._payload

    def raise_for_status(self) -> None:  # mimic requests.Response
        return None


class FakeSession:
    def __init__(self):
        self.headers = {}
        self.calls: list[tuple[str, str]] = []

    def post(self, url: str, *args: Any, **kwargs: Any):  # type: ignore[override]
        self.calls.append(("POST", url))
        if url.endswith("/oauth2/token"):
            return FakeResponse(200, {"access_token": "t"})
        if url.endswith("/upload"):
            return FakeResponse(500, text="boom")
        return FakeResponse(201, {"id": "new"})

    def get(self, url: str, *args: Any, **kwargs: Any):  # type: ignore[override]
        self.calls.append(("GET", url))
        params_obj = kwargs.get("params", {})
        if isinstance(params_obj, dict):
            params: dict[str, Any] = {str(k): v for k, v in params_obj.items()}
        else:
            params = {}
        path_val: Any = params.get("path") if "path" in params else None
        path = str(path_val) if path_val is not None else None
        if path == "does-not-exist":
            return FakeResponse(404, text="nope")
        if path == "return-500":
            return FakeResponse(500, text="boom")
        return FakeResponse(200, {"items": [{"id": "root"}]})

    def delete(self, url: str, *args: Any, **kwargs: Any):  # type: ignore[override]
        self.calls.append(("DELETE", url))
        if url.endswith("/items/xyz"):
            return FakeResponse(404, text="nope")
        if url.endswith("/items/bad"):
            return FakeResponse(500, text="boom")
        return FakeResponse(204)


class NoTimeoutClient(LibraryClient):
    # Override to inject our FakeSession without going through monkeypatch fixture
    def __init__(self, spotfire_url: str):
        self._url = f"{spotfire_url.rstrip('/')}/spotfire"
        self._requests_session = FakeSession()  # type: ignore[assignment]
        # perform authenticate like original
        from spotfire_community._core.rest.auth import authenticate
        from spotfire_community._core.rest.models import Scope

        authenticate(
            requests_session=self._requests_session,  # type: ignore[arg-type]
            url=self._url,
            scopes=[Scope.LIBRARY_READ, Scope.LIBRARY_WRITE],
            client_id="id",
            client_secret="secret",
        )


def test_get_folder_errors_and_success():
    client = NoTimeoutClient("http://x")
    with pytest.raises(ItemNotFoundError):
        client._get_folder_id("does-not-exist")  # pyright: ignore[reportPrivateUsage]
    with pytest.raises(Exception):
        client._get_folder_id("return-500")  # pyright: ignore[reportPrivateUsage]
    assert client._get_folder_id("/") == "root"  # pyright: ignore[reportPrivateUsage]


def test_create_folder_and_upload_job_error():
    client = NoTimeoutClient("http://x")
    # create should 201
    new_id = client._create_folder("Title", "root")  # pyright: ignore[reportPrivateUsage]
    assert new_id == "new"

    # upload creation should raise due to 500 from FakeSession
    with pytest.raises(Exception):
        client._create_upload_job("T", ItemType.DXP, "root", "", False)  # pyright: ignore[reportPrivateUsage]


def test_delete_item_errors():
    client = NoTimeoutClient("http://x")
    with pytest.raises(ItemNotFoundError):
        client._delete_item_by_id("xyz")  # pyright: ignore[reportPrivateUsage]
    # 204 path
    client._delete_item_by_id("ok")  # pyright: ignore[reportPrivateUsage]
    # other error
    with pytest.raises(Exception):
        client._delete_item_by_id("bad")  # pyright: ignore[reportPrivateUsage]


def test_create_folder_error_branch():
    class ErrorOnItemsSession(FakeSession):
        def post(self, url: str, *args: Any, **kwargs: Any):  # type: ignore[override]
            if url.endswith("/oauth2/token"):
                return FakeResponse(200, {"access_token": "t"})
            if url.endswith("/api/rest/library/v2/items"):
                return FakeResponse(400, text="bad")
            return super().post(url, *args, **kwargs)

    class ClientWithErrorItems(LibraryClient):
        def __init__(self, spotfire_url: str):
            self._url = f"{spotfire_url.rstrip('/')}/spotfire"
            self._requests_session = ErrorOnItemsSession()  # type: ignore[assignment]
            from spotfire_community._core.rest.auth import authenticate
            from spotfire_community._core.rest.models import Scope

            authenticate(
                requests_session=self._requests_session,  # type: ignore[arg-type]
                url=self._url,
                scopes=[Scope.LIBRARY_READ, Scope.LIBRARY_WRITE],
                client_id="id",
                client_secret="secret",
            )

    client = ClientWithErrorItems("http://x")
    with pytest.raises(Exception):
        client._create_folder("t", "root")  # pyright: ignore[reportPrivateUsage]


def test_get_or_create_folder_final_none_branch():
    class WeirdClient(LibraryClient):
        def __init__(self):
            # skip base init; only override methods used
            pass

        def _get_folder_id(self, path: str) -> str:  # type: ignore[override]
            if path == "/":
                return "root"
            raise ItemNotFoundError("nf")

        def _create_folder(
            self, title: str, parent_id: str, *, description: str = ""
        ) -> str | None:  # type: ignore[override]
            # Simulate failure to create returning None
            return None

    wc = WeirdClient()
    with pytest.raises(ItemNotFoundError):
        wc._get_or_create_folder("/foo")  # pyright: ignore[reportPrivateUsage]
