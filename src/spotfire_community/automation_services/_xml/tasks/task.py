from xml.etree.ElementTree import Element

from .models import TaskAttribute


class Task:
    element_name: str
    namespace: str
    title: str

    def __init__(self, namespace: str | None):
        self.namespace = namespace or "urn:tibco:spotfire.dxp.automation.tasks"

    def _get_title_element(self) -> Element:
        element = Element("as:Title")
        element.text = self.title
        return element

    def _build_attribute_element(self, attribute: TaskAttribute) -> Element:
        element = Element(attribute.name)
        element.text = attribute.value
        return element

    def attribute_generator(self) -> list[TaskAttribute]:
        return []

    def serialize(self) -> Element:
        element = Element(self.element_name)
        element.set("xmlns", self.namespace)
        element.append(self._get_title_element())
        for attr_element in self.attribute_generator():
            element.append(self._build_attribute_element(attr_element))
        return element
