"""Shared authentication helper for Spotfire REST clients."""

from requests.exceptions import RequestException

from .spotfire_requests import SpotfireRequestsSession
from .models import Scope


def authenticate(
    requests_session: SpotfireRequestsSession,
    url: str,
    scopes: list[Scope],
    client_id: str,
    client_secret: str,
) -> None:
    """Authenticate against Spotfire and set Bearer token on the session.

    Raises Exception on connection failures, non-200 responses, or missing token.
    """
    # Try to get the token to check if the credentials are valid
    try:
        token_response = requests_session.post(
            f"{url}/oauth2/token",
            auth=(client_id, client_secret),
            params={
                "grant_type": "client_credentials",
                "scope": " ".join([scope.value for scope in scopes]),
            },
        )
        token_response.raise_for_status()
    except RequestException as e:
        raise Exception(f"Failed to connect to Spotfire server: {e}")

    if token_response.status_code != 200:
        raise Exception(
            f"Failed to authenticate with Spotfire server: {token_response.status_code} - {token_response.text}"
        )

    if (token := token_response.json().get("access_token")) is None:
        raise Exception("No access token found in response.")

    requests_session.headers.update(
        {
            "Authorization": f"Bearer {token}",
            "Accept": "application/json",
        }
    )


__all__ = [
    "authenticate",
]
