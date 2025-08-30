from dataclasses import dataclass


@dataclass
class OAuthResponse:
    access_token: str
    token_type: str


__all__ = [
    "OAuthResponse",
]
