from enum import StrEnum


class ItemType(StrEnum):
    """
    Enum for the different types of items in the Spotfire library.

    Attributes:
        FOLDER (str): Represents a Spotfire folder.
        DXP (str): Represents a Spotfire DXP file.
    """

    FOLDER = "spotfire.folder"
    DXP = "spotfire.dxp"


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


__all__ = [
    "ItemType",
    "ConflictResolution",
]
