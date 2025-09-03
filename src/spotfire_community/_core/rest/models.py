"""Shared REST models used by Spotfire client code."""

from enum import StrEnum
from pydantic import BaseModel, ConfigDict


class Scope(StrEnum):
    """
    Enum for the different scopes of the Spotfire REST API.

    Attributes:
        LIBRARY_READ (str): Scope for reading the library.
        LIBRARY_WRITE (str): Scope for writing to the library.
    """

    LIBRARY_READ = "api.library.read"
    LIBRARY_WRITE = "api.library.write"
    AUTOMATION_SERVICES_EXECUTE = "api.rest.automation-services-job.execute"


class User(BaseModel):
    """
    Model representing a Spotfire user.

    Attributes:
        id (str): The unique identifier of the user.
        name (str): The name of the user.
        domain_name (str): The domain name of the user.
        display_name (str): The display name of the user.
    """

    model_config = ConfigDict(
        alias_generator=lambda s: s.split("_")[0]
        + "".join(part.title() for part in s.split("_")[1:]),
        populate_by_name=True,
    )

    id: str
    name: str
    domain_name: str
    display_name: str
