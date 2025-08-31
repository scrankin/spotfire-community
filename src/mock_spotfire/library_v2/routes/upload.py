from typing import Any

from fastapi import APIRouter, HTTPException, Query, Request
from fastapi.responses import JSONResponse

from ..models import LibraryItem, UploadJob
from ..state import state


router = APIRouter()


@router.post("/spotfire/api/rest/library/v2/upload")
def create_upload(payload: dict[str, Any]):
    """Create an upload job to upload content in one or more chunks."""
    overwrite = payload.get("overwriteIfExists", False)
    item_payload = payload.get("item", {})
    title = item_payload.get("title")
    item_type = item_payload.get("type")
    parent_id = item_payload.get("parentId")
    description = item_payload.get("description", "")

    if not title or not item_type or not parent_id:
        raise HTTPException(status_code=400, detail="Missing required fields")
    if parent_id not in state.items:
        raise HTTPException(status_code=404, detail="Parent not found")

    import uuid as _uuid

    job_id = str(_uuid.uuid4())
    job = UploadJob(
        jobId=job_id,
        item=LibraryItem(
            id=str(_uuid.uuid4()),
            title=title,
            type=item_type,
            parentId=parent_id,
            description=description,
        ),
        overwriteIfExists=overwrite,
    )
    state.upload_jobs[job_id] = job
    return JSONResponse(status_code=201, content={"jobId": job_id})


@router.post("/spotfire/api/rest/library/v2/upload/{job_id}")
async def upload_chunk(
    job_id: str,
    request: Request,
    chunk_index: int = Query(1, alias="chunk"),
    finish: bool = Query(False, alias="finish"),
):
    """Upload a chunk for an upload job; finalize when ``finish`` is true."""
    if job_id not in state.upload_jobs:
        raise HTTPException(status_code=404, detail="Job not found")
    job = state.upload_jobs[job_id]

    data = await request.body()
    job.chunks.append(data)

    if finish:
        # derive path
        parent_path = "/"
        if job.item.parentId != state.root_id:
            for p, i in state.path_index.items():
                if i == job.item.parentId:
                    parent_path = p
                    break
        target_path = parent_path.rstrip("/") + "/" + job.item.title

        # handle overwrite
        if target_path in state.path_index and not job.overwriteIfExists:
            raise HTTPException(
                status_code=409, detail="Item exists and overwrite=false"
            )
        if target_path in state.path_index and job.overwriteIfExists:
            existing_id = state.path_index[target_path]
            # Preserve existing ID; replace content
            job.item.id = existing_id
            state.items[existing_id] = job.item
            item_id = existing_id
        else:
            state.items[job.item.id] = job.item
            state.path_index[target_path] = job.item.id
            item_id = job.item.id

        del state.upload_jobs[job_id]
        return {"item": {"id": item_id}}

    return {"status": "chunk received", "chunk": chunk_index}


__all__ = ["router"]
