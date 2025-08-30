"""Shared REST models used by Spotfire client code."""

from enum import StrEnum


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
