from io import BytesIO

from . import serializer
from ._xml.data_access_plan import DataAccessPlan


class Dxp:
    """
    Represents a Spotfire DXP file or folder, providing methods to manipulate its contents.
    """

    _cleanup_required = False
    _root_path: str
    data_access_plan: DataAccessPlan

    def __init__(
        self,
        root_path: str,
    ):
        """
        Initializes a Dxp object from a root path or .dxp file.

        Args:
            root_path (str): Path to a DXP folder or .dxp file.
        """
        self._root_path, self._cleanup_required = (
            serializer.get_or_create_dxp_root_path(root_path)
        )
        self.data_access_plan = DataAccessPlan(self._root_path)

    def __del__(self):
        """
        Cleans up temporary files if required when the object is deleted.
        """
        if self._cleanup_required:
            serializer.remove_temporary_files(self._root_path)
            self._cleanup_required = False

    def unzip_to_path(self, output_path: str):
        """
        Copies the contents of the DXP root folder to the specified output directory.

        Args:
            output_path (str): Path to the output directory.

        Raises:
            InvalidDxpRootPathError: If the root path is not a valid DXP folder.
        """
        serializer.unzip_to_path(dxp_root_path=self._root_path, output_path=output_path)

    def get_all_files(self) -> list[str]:
        """
        Lists all files in the DXP root directory and its subdirectories.

        Returns:
            list[str]: A list of file paths.
        """
        return serializer.get_all_files(self._root_path)

    def get_zip_folder_in_memory(self) -> BytesIO:
        """
        Zips the DXP root folder into an in-memory BytesIO object.

        Returns:
            BytesIO: In-memory zip file.
        """
        return serializer.zip_in_memory(
            dxp_root_path=self._root_path,
        )

    def update_data_connection(
        self,
        data_connection_name: str,
        database: str,
        host: str,
    ):
        """
        Updates the data connection in the DataAccessPlan.xml file.

        Args:
            data_connection_name (str): Name of the data connection to update.
            database (str): New database value.
            host (str): New host value.

        Raises:
            DataConnectionNotFoundError: If the data connection is not found.
        """
        data_connection = self.data_access_plan.get_data_connection(
            data_connection_name
        )

        data_connection.database = database
        data_connection.host = host

        self.data_access_plan.save(self._root_path)


__all__ = ["Dxp"]
