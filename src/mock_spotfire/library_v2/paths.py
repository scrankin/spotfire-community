import uuid
from typing import Any

from fastapi import APIRouter, HTTPException, Query, Request
from fastapi.responses import JSONResponse

from .errors import ErrorCode, error_response
from .models import LibraryItem, UploadJob
from .state import state


router = APIRouter()


@router.get("/spotfire/api/rest/library/v2/items")
def get_items(
    path: str | None = Query(None),
    item_type: str | None = Query(None, alias="type"),
    maxResults: int | None = Query(None),
) -> Any:
    """List items by path or type.

    Special cases for testing:
    - path == "return-500" -> raises 500
    - unknown path -> 404
    """
    # For testing
    if path == "return-500":
        raise HTTPException(status_code=500, detail="Fake Internal Server Error")
    if path is not None:
        item_id = state.get_path(path)
        if item_id is None:
            raise HTTPException(status_code=404, detail="Item not found")
        item = state.items[item_id]
        return {"items": [{"id": item.id, "title": item.title, "type": item.type}]}

    items = [
        {"id": i.id, "title": i.title, "type": i.type}
        for i in state.items.values()
        if (item_type is None or i.type == item_type)
    ]
    if maxResults is not None:
        items = items[: max(0, maxResults)]
    return {"items": items}


@router.post("/spotfire/api/rest/library/v2/items")
def create_item(payload: dict[str, Any]) -> JSONResponse:
    """Create a new library item under a parent folder."""
    title = payload.get("title")
    if title == "return-500":
        raise HTTPException(status_code=500, detail="Fake Internal Server Error")

    item_type = payload.get("type")
    parent_id = payload.get("parentId")
    description = payload.get("description", "")
    if not title or not item_type or not parent_id:
        raise HTTPException(status_code=400, detail="Missing required fields")
    if parent_id not in state.items:
        raise HTTPException(status_code=404, detail="Parent not found")

    # path
    parent_path = "/"
    if parent_id != state.root_id:
        for p, i in state.path_index.items():
            if i == parent_id:
                parent_path = p
                break
    new_path = parent_path.rstrip("/") + "/" + title
    if new_path in state.path_index:
        return error_response(
            status_code=409,
            code=ErrorCode.CONFLICT,
            message="Item exists",
        )

    new_id = str(uuid.uuid4())
    item = LibraryItem(
        id=new_id,
        title=title,
        type=item_type,
        parentId=parent_id,
        description=description,
    )
    state.items[new_id] = item
    state.path_index[new_path] = new_id
    return JSONResponse(status_code=201, content={"id": new_id})


@router.delete("/spotfire/api/rest/library/v2/items/{item_id}")
def delete_item(item_id: str):
    """Delete an item by id, including its subtree."""
    if item_id not in state.items:
        raise HTTPException(status_code=404, detail="Item not found")
    if item_id == state.root_id:
        raise HTTPException(status_code=400, detail="Cannot delete root")

    # collect subtree
    to_delete = [item_id]
    for iid, itm in list(state.items.items()):
        cur = itm
        while cur.parentId != "root":
            if cur.parentId == item_id:
                to_delete.append(iid)
                break
            cur = state.items.get(cur.parentId)  # type: ignore
            if cur is None:
                break

    # remove paths and items
    for p, i in list(state.path_index.items()):
        if i in to_delete:
            del state.path_index[p]
    for i in to_delete:
        if i in state.items:
            del state.items[i]

    return JSONResponse(status_code=204, content=None)


@router.post("/spotfire/api/rest/library/v2/upload")
def create_upload(payload: dict[str, Any]) -> JSONResponse:
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

    job_id = str(uuid.uuid4())
    job = UploadJob(
        jobId=job_id,
        item=LibraryItem(
            id=str(uuid.uuid4()),
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


__all__ = [
    "router",
]
