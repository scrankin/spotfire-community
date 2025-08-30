"""Task that opens an analysis from the Spotfire library."""

from typing import Optional
from xml.etree.ElementTree import Element

from .task import Task


class OpenAnalysisTask(Task):
    """Serialize an OpenAnalysisFromLibrary task.

    Args:
        path: Library path to the analysis (e.g., "/Samples/Analysis.dxp").
        configuration_block: Optional configuration block XML/text.
    """

    path: str
    configuration_block: str

    def __init__(
        self,
        path: str,
        *,
        configuration_block: str = "",
        namespace: Optional[str] = None,
    ):
        self.path = path
        self.configuration_block = configuration_block
        super().__init__(namespace)

    @property
    def name(self) -> str:
        return "OpenAnalysisFromLibrary"

    @property
    def title(self) -> str:
        return "Open Analysis from Library"

    def build_attribute_elements(self) -> list[Element]:
        attribute_elements: list[Element] = [
            self.build_element("AnalysisPath", self.path),
            self.build_element("ConfigurationBlock", self.configuration_block),
        ]

        return attribute_elements
