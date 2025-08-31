from fastapi import APIRouter

from .status import router as status_router
from .abort import router as abort_router
from .start_xml import router as start_xml_router
from .start_library import router as start_library_router
from .test_hooks import router as test_hooks_router


router = APIRouter()

router.include_router(status_router)
router.include_router(abort_router)
router.include_router(start_xml_router)
router.include_router(start_library_router)
router.include_router(test_hooks_router)


__all__ = ["router"]
