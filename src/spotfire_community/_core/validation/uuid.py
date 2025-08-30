"""Validation helpers for common types used by the clients."""

import uuid


def is_valid_uuid(
    value: str,
    *,
    version: int = 4,
) -> bool:
    """Return True if value is a valid UUID of the specified version."""
    try:
        uuid_obj = uuid.UUID(value, version=version)
    except (ValueError, AttributeError, TypeError):
        return False
    return str(uuid_obj) == value.lower()


__all__ = [
    "is_valid_uuid",
]
