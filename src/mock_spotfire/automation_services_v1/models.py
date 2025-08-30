from dataclasses import dataclass, field
import time

from spotfire_community.automation_services.models import ExecutionStatus


@dataclass
class Job:
    """Represents an in-memory job in the mock service."""

    id: str
    status: ExecutionStatus
    created_at: float = field(default_factory=time.monotonic)


@dataclass
class JobDefinition:
    """Represents a library Automation Services job definition entry."""

    id: str
    library_path: str


__all__ = [
    "Job",
    "JobDefinition",
]
