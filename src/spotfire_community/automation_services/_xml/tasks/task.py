"""Abstract base for Automation Services XML tasks."""

from abc import ABC, abstractmethod
from typing import Optional, final
from xml.etree.ElementTree import Element


class Task(ABC):
    """Base class for all Automation Services tasks.

    Subclasses must implement ``name``, ``title`` and
    ``build_attribute_elements`` to produce the expected task XML.
    """

    namespace: str

    def __init__(self, namespace: Optional[str] = None):
        self.namespace = namespace or "urn:tibco:spotfire.dxp.automation.tasks"

    @property
    @abstractmethod
    def name(self) -> str: ...

    @property
    @abstractmethod
    def title(self) -> str: ...

    @abstractmethod
    def build_attribute_elements(self) -> list[Element]: ...

    @final
    def _get_title_element(self) -> Element:
        """Return the standard <as:Title> element for this task."""
        element = Element("as:Title")
        element.text = self.title
        return element

    @final
    def build_element(self, name: str, inner_text: str) -> Element:
        """Utility to build a simple text element."""
        element = Element(name)
        element.text = inner_text
        return element

    @final
    def serialize(self) -> Element:
        """Serialize this task to an XML Element."""
        element = Element(self.name)
        element.set("xmlns", self.namespace)
        element.append(self._get_title_element())
        for attribute_element in self.build_attribute_elements():
            element.append(attribute_element)
        return element
