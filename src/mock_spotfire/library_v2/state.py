"""In-memory state store backing the mock Library v2 endpoints."""

import uuid

from .models import LibraryItem, UploadJob


class LibraryState:
    """Holds library items, path index and active upload jobs for tests."""

    def __init__(self):
        # Root folder
        root_id = str(uuid.uuid4())
        self.root_id = root_id
        self.items: dict[str, LibraryItem] = {
            root_id: LibraryItem(
                id=root_id,
                title="/",
                type="spotfire.folder",
                parentId="root",
            )
        }
        self.path_index: dict[str, str] = {"/": root_id}
        self.upload_jobs: dict[str, UploadJob] = {}

    def get_path(self, path: str) -> str | None:
        """Return the item id for a given path, if any."""
        return self.path_index.get(path)


# Singleton state used by handlers
state = LibraryState()

__all__ = ["LibraryState", "state"]
