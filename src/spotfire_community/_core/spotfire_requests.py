from requests import Session, Response
from typing import Any


class SpotfireRequestsSession(Session):
    def __init__(self, timeout: float | None = None):
        super().__init__()
        self.timeout = timeout

    def request(
        self, method: str | bytes, url: str, *args: Any, **kwargs: Any
    ) -> Response:
        if self.timeout is not None:
            kwargs.setdefault("timeout", self.timeout)
        return super().request(method, url, *args, **kwargs)


__all__ = [
    "SpotfireRequestsSession",
]
