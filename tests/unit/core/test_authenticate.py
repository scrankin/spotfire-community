from typing import Any

import pytest

from spotfire_community._core.rest.auth import authenticate
from spotfire_community._core.rest.models import Scope


class StubResponse:
    def __init__(self, status_code: int, json_payload: dict[str, Any] | None = None):
        self.status_code = status_code
        self._json = json_payload or {}
        self.text = ""

    def raise_for_status(self) -> None:
        # Do not raise; let caller inspect status_code to cover branch
        return None

    def json(self) -> dict[str, Any]:  # type: ignore[override]
        return self._json


class StubSession:
    def __init__(self):
        self.headers: dict[str, str] = {}

    def post(self, url: str, *args: Any, **kwargs: Any) -> StubResponse:  # type: ignore[override]
        # Decide behavior based on client id embedded in auth tuple
        auth = kwargs.get("auth", ("", ""))
        client_id = auth[0]
        if client_id == "non200":
            return StubResponse(202, {"note": "no token"})
        if client_id == "missingtoken":
            return StubResponse(200, {"not_access_token": "nope"})
        return StubResponse(200, {"access_token": "t"})


def test_authenticate_non_200_raises():
    s = StubSession()
    with pytest.raises(Exception):
        authenticate(
            requests_session=s,  # type: ignore[arg-type]
            url="http://x",
            scopes=[Scope.LIBRARY_READ],
            client_id="non200",
            client_secret="b",
        )


def test_authenticate_missing_token_raises():
    s = StubSession()
    with pytest.raises(Exception):
        authenticate(
            requests_session=s,  # type: ignore[arg-type]
            url="http://x",
            scopes=[Scope.LIBRARY_READ],
            client_id="missingtoken",
            client_secret="b",
        )


def test_authenticate_success_sets_headers():
    s = StubSession()
    authenticate(
        requests_session=s,  # type: ignore[arg-type]
        url="http://x",
        scopes=[Scope.LIBRARY_READ],
        client_id="a",
        client_secret="b",
    )
    assert s.headers["Authorization"].startswith("Bearer ")
    assert s.headers["Accept"] == "application/json"


def test_authenticate_request_exception(monkeypatch: "pytest.MonkeyPatch"):
    class RaisingSession(StubSession):
        def post(self, *args: Any, **kwargs: Any):  # type: ignore[override]
            from requests.exceptions import RequestException

            raise RequestException("network down")

    s = RaisingSession()
    with pytest.raises(Exception):
        authenticate(
            requests_session=s,  # type: ignore[arg-type]
            url="http://x",
            scopes=[Scope.LIBRARY_READ],
            client_id="a",
            client_secret="b",
        )
