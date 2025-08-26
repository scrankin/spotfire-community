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


class Scope(StrEnum):
    """
    Enum for the different scopes of the Spotfire REST API.

    Attributes:
        LIBRARY_READ (str): Scope for reading the library.
        LIBRARY_WRITE (str): Scope for writing to the library.
    """

    LIBRARY_READ = "api.library.read"
    LIBRARY_WRITE = "api.library.write"


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
    "Scope",
    "ConflictResolution",
]
