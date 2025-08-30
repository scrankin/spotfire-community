"""XML serializers for Automation Services JobDefinition payloads."""

from xml.etree.ElementTree import Element, tostring

from .tasks import Task


class JobDefinition:
    """Container for a sequence of Automation Services tasks.

    Provides serialization to the XML format expected by the REST API.
    """

    _tasks: list[Task]

    def __init__(self):
        self._tasks = []

    def add_task(self, task: Task):
        """Append a task to the definition in execution order."""
        self._tasks.append(task)

    def get_tasks(self) -> list[Task]:
        """Return the current list of tasks."""
        return self._tasks

    def serialize(self) -> Element:
        """Serialize this JobDefinition into an XML Element."""
        element = Element("as:Job")
        element.set("xmlns:as", "urn:tibco:spotfire.dxp.automation")
        tasks_element = Element("as:Tasks")
        for task in self._tasks:
            tasks_element.append(task.serialize())
        element.append(tasks_element)
        return element

    def as_bytes(self) -> bytes:
        """Return the serialized XML as bytes with declaration."""
        return tostring(self.serialize(), encoding="utf-8", xml_declaration=True)
