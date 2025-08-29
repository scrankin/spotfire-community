from typing import Optional
from xml.etree.ElementTree import Element

from .task import Task


class ApplyBookmarkTask(Task):
    _use_bookmark_name: bool
    bookmark_id: str
    bookmark_name: str

    def __init__(
        self,
        *,
        bookmark_id: Optional[str] = None,
        bookmark_name: Optional[str] = None,
        namespace: Optional[str] = None,
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

    @property
    def name(self) -> str:
        return "ApplyBookmark"

    @property
    def title(self) -> str:
        return "Apply Bookmark"

    def build_attribute_elements(self) -> list[Element]:
        attribute_elements: list[Element] = [
            self.build_element("BookmarkId", self.bookmark_id),
            self.build_element("BookmarkName", self.bookmark_name),
            self.build_element("UseBookmarkName", str(self._use_bookmark_name).lower()),
        ]

        return attribute_elements
