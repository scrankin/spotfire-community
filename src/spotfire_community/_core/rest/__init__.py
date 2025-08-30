"""Public exports for REST utilities (auth, session, scopes)."""

from .models import Scope
from .auth import authenticate
from .spotfire_requests import SpotfireRequestsSession


__all__ = [
    "Scope",
    "authenticate",
    "SpotfireRequestsSession",
]
