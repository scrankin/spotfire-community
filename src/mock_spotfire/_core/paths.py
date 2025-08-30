"""Core utility endpoints for the mock API (e.g., OAuth token)."""

from fastapi import Query, Request, HTTPException

from .models import OAuthResponse


def oauth2_token(
    request: Request,
    grant_type: str = Query("client_credentials"),
    scope: str = Query(""),
) -> OAuthResponse:
    """Return a mock OAuth token; supports special basic auth triggers.

    If the Authorization header matches specific base64-encoded values, this
    endpoint deliberately raises a 500 or 202 for test coverage hooks.
    """
    authorization = request.headers.get("authorization")

    # Test authorization for triggering 500
    if authorization == "Basic cmV0dXJuLTUwMDpyZXR1cm4tNTAw":
        raise HTTPException(
            status_code=500,
            detail="This is a test for triggering an internal server error.",
        )
    # Test authorization for triggering 202
    elif authorization == "Basic cmV0dXJuLTIwMjpyZXR1cm4tMjAy":
        raise HTTPException(
            status_code=202,
            detail="This is a test for triggering a successful response.",
        )

    # Minimal token endpoint to satisfy LibraryClient authentication.
    return OAuthResponse(access_token="mock-token", token_type="bearer")


__all__ = [
    "oauth2_token",
]
