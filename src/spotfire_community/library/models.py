from enum import StrEnum
from pydantic import BaseModel, ConfigDict
from typing import Optional

from .._core.rest.models import User


class ItemType(StrEnum):
    """
    Enum for the different types of items in the Spotfire library.

    Attributes:
        FOLDER (str): Represents a Spotfire folder.
        DXP (str): Represents a Spotfire DXP file.
    """

    DXP = "spotfire.dxp"
    FOLDER = "spotfire.folder"
    MOD = "spotfire.mod"


class ConflictResolution(StrEnum):
    """
    Enum for the different conflict resolution strategies.

    Attributes:
        KEEP_OLD (str): Keep the old item in case of conflict.
        KEEP_NEW (str): Keep the new item in case of conflict.
        KEEP_BOTH (str): Keep both items in case of conflict.
    """

    KEEP_OLD = "KeepOld"
    KEEP_NEW = "KeepNew"
    KEEP_BOTH = "KeepBoth"


class LibraryItem(BaseModel):
    """
    Base model for library items.
    """

    model_config = ConfigDict(
        alias_generator=lambda s: s.split("_")[0]
        + "".join(part.title() for part in s.split("_")[1:]),
        populate_by_name=True,
    )

    id: str
    title: str
    type: ItemType
    created_by: User
    created: int
    modified_by: User
    modified: int
    parent_id: str
    size: int
    version_id: str
    is_favorite: bool

    accessed: Optional[int] = None
    path: Optional[str] = None
    description: Optional[str] = None


__all__ = [
    "ItemType",
    "ConflictResolution",
    "LibraryItem",
]
