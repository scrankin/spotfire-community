"""Pydantic-lite dataclasses used by the mock Library v2 server."""

from dataclasses import dataclass, field
from enum import Enum


@dataclass
class UserPrincipal:
    id: str
    name: str
    domainName: str
    displayName: str


@dataclass
class LibraryProperty:
    key: str | None
    values: list[str] | None
    versioned: bool | None


class PrincipalType(Enum):
    USER = "user"
    GROUP = "group"


class LibraryPermissions(Enum):
    READ = "read"
    WRITE = "write"
    OWNER = "owner"
    EXECUTE = "execute"


@dataclass
class AclEntry:
    principalId: str | None
    principalName: str | None
    domainName: str | None
    principalType: PrincipalType | None
    permissions: list[LibraryPermissions] | None
    inheritedFromId: str | None


@dataclass
class LibraryItemVersion:
    id: str
    created: str
    createdBy: UserPrincipal | None
    formatVersion: str | None
    size: int | None
    name: str | None
    comment: str | None
    restoredFromVersionId: str | None
    restoredFromTimestamp: str | None


@dataclass
class LibraryItem:
    id: str
    title: str
    type: str
    parentId: str
    description: str | None = None
    createdBy: UserPrincipal | None = None
    created: str | None = None
    modifiedBy: UserPrincipal | None = None
    modified: str | None = None
    accessed: str | None = None
    size: int | None = None
    properties: list[LibraryProperty] | None = None
    permissions: list[AclEntry] | None = None
    versionId: str | None = None
    itemVersions: list[LibraryItemVersion] | None = None
    isFavorite: bool | None = None
    favoriteCount: int | None = None


@dataclass
class UploadJob:
    jobId: str
    item: LibraryItem
    overwriteIfExists: bool
    # default_factory must be a callable; use a lambda for precise typing
    # using just default_factory=list causes linting errors
    chunks: list[bytes] = field(default_factory=lambda: list[bytes]())


__all__ = [
    "UserPrincipal",
    "LibraryProperty",
    "PrincipalType",
    "LibraryPermissions",
    "AclEntry",
    "LibraryItemVersion",
    "LibraryItem",
    "UploadJob",
]
