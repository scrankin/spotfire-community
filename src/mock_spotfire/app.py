from fastapi import FastAPI


from .automation_services_v1.paths import router as automation_services_v1_router
from .library_v2 import router as library_v2_router
from ._core import router as core_router


app = FastAPI()
app.include_router(automation_services_v1_router)
app.include_router(library_v2_router)
app.include_router(core_router)


__all__ = ["app"]
