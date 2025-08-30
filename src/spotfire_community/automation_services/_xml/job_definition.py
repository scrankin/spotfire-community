from xml.etree.ElementTree import Element, tostring

from .tasks import Task


class JobDefinition:
    _tasks: list[Task]

    def __init__(self):
        self._tasks = []

    def add_task(self, task: Task):
        self._tasks.append(task)

    def get_tasks(self) -> list[Task]:
        return self._tasks

    def serialize(self) -> Element:
        element = Element("as:Job")
        element.set("xmlns:as", "urn:tibco:spotfire.dxp.automation.jobs")
        tasks_element = Element("as:Tasks")
        for task in self._tasks:
            tasks_element.append(task.serialize())
        element.append(tasks_element)
        return element

    def as_bytes(self) -> bytes:
        return tostring(self.serialize(), encoding="utf-8", xml_declaration=True)
