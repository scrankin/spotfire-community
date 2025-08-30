from fastapi import APIRouter

from .items import router as items_router
from .upload import router as upload_router


router = APIRouter()

# Include sub-routers under the same base paths they already declare
router.include_router(items_router)
router.include_router(upload_router)


__all__ = ["router"]
