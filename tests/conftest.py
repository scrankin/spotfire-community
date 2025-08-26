import sys
from pathlib import Path
from typing import Any

import httpx
import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from pytest import MonkeyPatch

# Ensure src is on sys.path for imports
ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from mock_spotfire.library_v2 import app  # noqa: E402


class RequestsCompatibleTestClient(TestClient):
    def __init__(self, app: FastAPI):
        super().__init__(app)

    def request(
        self, method: str, url: str, *args: Any, **kwargs: Any
    ) -> httpx.Response:  # type: ignore[override]
        # Remove timeout if present (TestClient doesn't accept it)
        kwargs.pop("timeout", None)
        return super().request(method, url, *args, **kwargs)

    def post(self, url: httpx._types.URLTypes, *args: Any, **kwargs: Any):  # type: ignore[override]
        data = kwargs.pop("data", None)  # type: ignore[assignment]
        # Remove timeout if present
        kwargs.pop("timeout", None)
        return super().post(url, content=data, *args, **kwargs)


@pytest.fixture()
def test_client() -> RequestsCompatibleTestClient:
    return RequestsCompatibleTestClient(app)


@pytest.fixture(autouse=True)
def patch_requests_session(monkeypatch: MonkeyPatch, test_client: TestClient):
    import spotfire_community.library.client as lib_client

    monkeypatch.setattr(
        lib_client,
        "SpotfireRequestsSession",
        lambda timeout=None: test_client,  # type: ignore[misc]
    )
    yield
