import uuid


def is_valid_uuid(
    value: str,
    *,
    version: int = 4,
) -> bool:
    try:
        uuid_obj = uuid.UUID(value, version=version)
    except (ValueError, AttributeError, TypeError):
        return False
    return str(uuid_obj) == value.lower()


__all__ = [
    "is_valid_uuid",
]
