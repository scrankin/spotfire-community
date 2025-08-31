from typing import Any, Dict

from spotfire_community._core.rest.spotfire_requests import SpotfireRequestsSession
from pytest import MonkeyPatch


class DummySession(SpotfireRequestsSession):
    def __init__(self, timeout: float | None):
        super().__init__(timeout=timeout)
        self._last_kwargs: dict[str, Any] | None = None

    def request(self, method: str | bytes, url: str, *args: Any, **kwargs: Any):  # type: ignore[override]
        # call base to inject timeout if needed, but intercept kwargs
        return super().request(method, url, *args, **kwargs)


def test_timeout_injection(monkeypatch: MonkeyPatch):
    captured: Dict[str, Any] = {}

    def fake_super_request(self, method, url, *args, **kwargs):  # type: ignore[no-redef]
        captured.update(
            {k: v for k, v in kwargs.items()}
        )  # ensure concrete dict[str, Any]

        class R:  # minimal Response stand-in
            status_code = 200

        return R()

    # Patch Session.request on the MRO base
    import requests

    monkeypatch.setattr(requests.Session, "request", fake_super_request, raising=True)

    s = DummySession(timeout=2.5)
    s.request("GET", "http://example.com")
    assert captured.get("timeout") == 2.5

    captured.clear()
    s.request("GET", "http://example.com", timeout=9)
    # Existing timeout should be preserved
    assert captured.get("timeout") == 9
