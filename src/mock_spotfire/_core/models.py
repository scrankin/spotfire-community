"""Dataclasses used by the mock core endpoints (e.g., OAuth responses)."""

from dataclasses import dataclass


@dataclass
class OAuthResponse:
    access_token: str
    token_type: str


__all__ = [
    "OAuthResponse",
]
