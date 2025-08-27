from fastapi import Query

from .models import OAuthResponse


def oauth2_token(
    grant_type: str = Query("client_credentials"),
    scope: str = Query(""),
) -> OAuthResponse:
    # Minimal token endpoint to satisfy LibraryClient authentication.
    return OAuthResponse(access_token="mock-token", token_type="bearer")


__all__ = [
    "oauth2_token",
]
