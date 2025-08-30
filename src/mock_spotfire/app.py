"""FastAPI application wiring for the mock Spotfire APIs.

This app aggregates routers for:
- Core OAuth2 token endpoint (mock)
- Library v2 endpoints (mock)
- Automation Services v1 endpoints (mock)
"""

from fastapi import FastAPI


from .automation_services_v1 import router as automation_services_v1_router
from .library_v2 import router as library_v2_router
from ._core import router as core_router


app = FastAPI(title="Mock Spotfire APIs")
app.include_router(automation_services_v1_router)
app.include_router(library_v2_router)
app.include_router(core_router)


__all__ = ["app"]
