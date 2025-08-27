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
    except RequestException as e:
        raise Exception(f"Failed to connect to Spotfire server: {e}")

    if token_response.status_code != 200:
        raise Exception(
            f"Failed to authenticate with Spotfire server: {token_response.status_code} - {token_response.text}"
        )

    requests_session.headers.update(
        {
            "Authorization": f"Bearer {token_response.json()['access_token']}",
            "Accept": "application/json",
        }
    )


__all__ = [
    "authenticate",
]
