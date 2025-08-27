from .models import TaskAttribute
from .task import Task


class ApplyBookmarkTask(Task):
    _use_bookmark_name: bool
    bookmark_id: str
    bookmark_name: str

    def __init__(
        self,
        bookmark_id: str | None,
        bookmark_name: str | None,
        namespace: str | None,
    ):
        self._use_bookmark_name = bookmark_id is None
        if bookmark_id is not None and bookmark_name is not None:
            raise Exception("Specify either bookmark_id or bookmark_name, not both.")
        elif bookmark_id is None and bookmark_name is None:
            raise Exception("Specify either bookmark_id or bookmark_name.")
        elif bookmark_id is not None:
            self.bookmark_id = bookmark_id
            self.bookmark_name = ""
        elif bookmark_name is not None:
            self.bookmark_id = "00000000-0000-0000-0000-000000000000"
            self.bookmark_name = bookmark_name
        super().__init__(namespace)

    def attribute_generator(self) -> list[TaskAttribute]:
        attributes: list[TaskAttribute] = [
            TaskAttribute(name="BookmarkId", value=self.bookmark_id),
            TaskAttribute(name="BookmarkName", value=self.bookmark_name),
            TaskAttribute(
                name="UseBookmarkName", value=str(self._use_bookmark_name).lower()
            ),
        ]
        return attributes
