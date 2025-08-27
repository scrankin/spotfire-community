from .._core.rest import SpotfireRequestsSession, authenticate, Scope


class AutomationServicesClient:
    _url: str
    _requests_session: SpotfireRequestsSession

    def __init__(
        self,
        spotfire_url: str,
        client_id: str,
        client_secret: str,
        *,
        timeout: float = 30.0,
    ):
        self._url = f"{spotfire_url.rstrip('/')}/spotfire"
        self._requests_session = SpotfireRequestsSession(timeout=timeout)

        authenticate(
            requests_session=self._requests_session,
            url=self._url,
            scopes=[Scope.AUTOMATION_SERVICES_EXECUTE],
            client_id=client_id,
            client_secret=client_secret,
        )

    def _start_job_with_definition(self):
        pass
