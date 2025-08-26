import os
import xml.etree.ElementTree as ET
from xml.etree.ElementTree import ElementTree

from .data_connection import DataConnection
from ..errors import DataConnectionNotFoundError


class DataAccessPlan:
    """
    Represents the DataAccessPlan.xml file in a Spotfire DXP archive, providing methods to access and modify data connections.
    """

    _xml: ElementTree

    def __init__(
        self,
        dxp_root_folder: str,
    ):
        """
        Initializes the DataAccessPlan by loading the XML from the DXP root folder.

        Args:
            dxp_root_folder (str): Path to the DXP root folder.
        """
        parsed_xml = ET.fromstring(self._get_file_in_dxp_folder(dxp_root_folder))
        self._xml = ElementTree(parsed_xml)

    def _get_file_in_dxp_folder(
        self,
        dxp_root_folder: str,
    ) -> str:
        """
        Reads the DataAccessPlan.xml file from the DXP root folder.

        Args:
            dxp_root_folder (str): Path to the DXP root folder.

        Returns:
            str: Contents of the DataAccessPlan.xml file.
        """
        file_text: str
        with open(
            os.path.join(dxp_root_folder, "DataArchive", "DataAccessPlan.xml"), "r"
        ) as data_access_plan_file:
            file_text = data_access_plan_file.read()
        return file_text

    def get_element_tree(self) -> ElementTree:
        """
        Returns the ElementTree object for the DataAccessPlan XML.

        Returns:
            ElementTree: The XML tree.
        """
        return self._xml

    def save(self, dxp_root_folder: str) -> None:
        """
        Saves the current state of the DataAccessPlan XML to the DXP root folder.

        Args:
            dxp_root_folder (str): Path to the DXP root folder.
        """
        with open(
            os.path.join(dxp_root_folder, "DataArchive", "DataAccessPlan.xml"), "wb"
        ) as data_access_plan_file:
            self._xml.write(data_access_plan_file)

    def get_data_all_connections(self) -> list[DataConnection]:
        """
        Returns all DataConnection objects found in the DataAccessPlan XML.

        Returns:
            list[DataConnection]: List of DataConnection objects.
        """
        data_connections: list[DataConnection] = []
        for conn_element in self._xml.findall(".//DataConnection"):
            data_connections.append(DataConnection(conn_element))
        return data_connections

    def get_data_connection(
        self,
        name: str,
    ) -> DataConnection:
        """
        Returns a DataConnection object by name.

        Args:
            name (str): Name of the data connection.

        Returns:
            DataConnection: The matching DataConnection object.

        Raises:
            DataConnectionNotFoundError: If the data connection is not found.
        """
        search = self._xml.find(f".//DataConnection[@Name='{name}']")
        if search is None:
            raise DataConnectionNotFoundError(f"Data connection '{name}' not found.")
        return DataConnection(search)


__all__ = ["DataAccessPlan"]
