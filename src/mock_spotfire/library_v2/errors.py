"""Error helpers used by the mock Library v2 API."""

from enum import Enum
from fastapi.responses import JSONResponse


class ErrorCode(str, Enum):
    NOT_FOUND = "not_found"
    CONFLICT = "conflict"


def error_payload(code: ErrorCode, message: str) -> dict[str, dict[str, str]]:
    """Build a JSON API error payload matching Spotfire's shape."""
    return {"error": {"code": code.value, "message": message}}


def error_response(status_code: int, code: ErrorCode, message: str) -> JSONResponse:
    """Return a JSONResponse with the error payload and status code."""
    return JSONResponse(status_code=status_code, content=error_payload(code, message))


__all__ = ["ErrorCode", "error_payload", "error_response"]
