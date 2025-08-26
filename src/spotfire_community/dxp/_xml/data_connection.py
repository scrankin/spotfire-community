from xml.etree.ElementTree import Element


class DataConnection:
    """
    Represents a data connection in the DataAccessPlan.xml file.
    """

    _xml_element: Element

    def __init__(self, xml_element: Element):
        """
        Initializes a DataConnection object from an XML element.

        Args:
            xml_element (Element): XML element representing the data connection.
        """
        self._xml_element = xml_element

    def get_element(self) -> Element:
        """
        Returns the underlying XML element for this data connection.

        Returns:
            Element: The XML element.
        """
        return self._xml_element

    @property
    def database(self) -> Element:
        """
        Gets the database element from the data connection XML.

        Returns:
            Element: The database XML element.

        Raises:
            ValueError: If the database element is not found.
        """
        search = self._xml_element.find(".//DataSourceLink/DataAdapter/Database")
        if search is None:
            raise ValueError("Database element not found in DataConnection XML.")
        return search

    @database.setter
    def database(self, value: str) -> None:
        """
        Sets the database value in the data connection XML.

        Args:
            value (str): The new database value.
        """
        self.database.text = value

    @property
    def host(self) -> Element:
        """
        Gets the host (data source) element from the data connection XML.

        Returns:
            Element: The data source XML element.

        Raises:
            ValueError: If the data source element is not found.
        """
        search = self._xml_element.find(".//DataSourceLink/DataAdapter/DataSource")
        if search is None:
            raise ValueError("DataSource element not found in DataConnection XML.")
        return search

    @host.setter
    def host(self, value: str) -> None:
        """
        Sets the host (data source) value in the data connection XML.

        Args:
            value (str): The new host value.
        """
        self.host.text = value


__all__ = ["DataConnection"]
