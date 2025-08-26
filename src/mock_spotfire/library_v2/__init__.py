from fastapi import FastAPI

from .paths import router

app = FastAPI(title="Mock Spotfire Library v2")
app.include_router(router)

__all__ = ["app"]
