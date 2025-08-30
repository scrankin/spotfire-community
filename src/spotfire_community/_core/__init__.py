"""Core utilities re-exported for use by subpackages and users."""

from .rest import authenticate, Scope, SpotfireRequestsSession
from .validation import is_valid_uuid


__all__ = [
    "SpotfireRequestsSession",
    "authenticate",
    "Scope",
    "is_valid_uuid",
]
