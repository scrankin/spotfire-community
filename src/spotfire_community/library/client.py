"""Client for Spotfire Library REST API (v2)."""

import logging
import requests
from .._core import SpotfireRequestsSession

from .models import (
    ItemType,
)
from .._core.rest import authenticate, Scope
from .errors import ItemNotFoundError


logger = logging.getLogger(__name__)


class LibraryClient:
    """
    Client for interacting with the Spotfire REST API.

    Provides methods to manage folders and files in the Spotfire library.
    """

    _url: str
    _requests_session: requests.Session

    def __init__(
        self,
        spotfire_url: str,
        client_id: str,
        client_secret: str,
        *,
        timeout: float = 30.0,
    ):
        """
        Initializes the Spotfire client and authenticates with the server.

        Args:
            spotfire_url (str): The base URL for the Spotfire server, e.g., https://dev.spotfire.com.
            client_id (str): The client ID for authentication.
            client_secret (str): The client secret for authentication.

        Raises:
            Exception: If authentication or connection fails.
        """
        self._url = f"{spotfire_url.rstrip('/')}/spotfire"

        self._requests_session = SpotfireRequestsSession(timeout=timeout)

        try:
            authenticate(
                requests_session=self._requests_session,
                url=self._url,
                scopes=[Scope.LIBRARY_READ, Scope.LIBRARY_WRITE],
                client_id=client_id,
                client_secret=client_secret,
            )
        except Exception as e:
            raise Exception(f"Failed to authenticate with Spotfire server: {e}")

    def _get_folder_id(self, path: str) -> str:
        """
        Gets the folder ID for a given path.

        Args:
            path (str): The path of the folder.

        Returns:
            str: The ID of the folder.

        Raises:
            ItemNotFoundError: If the folder is not found.
            Exception: For other errors returned by the API.
        """
        response = self._requests_session.get(
            f"{self._url}/api/rest/library/v2/items",
            params={
                "path": path,
                "type": ItemType.FOLDER,
                "maxResults": "1",
            },
        )

        if response.status_code == 404:
            raise ItemNotFoundError(f"Folder not found: {path}")
        elif response.status_code != 200:
            raise Exception(
                f"Error fetching folder ID: {response.status_code} - {response.text}"
            )

        data = response.json()

        return data["items"][0]["id"]

    def _create_folder(
        self,
        title: str,
        parent_id: str,
        *,
        description: str = "",
    ) -> str:
        """
        Creates a folder in the Spotfire library.

        Args:
            title (str): The title of the folder.
            parent_id (str): The ID of the parent folder.
            description (str): The description of the folder.

        Returns:
            str: The ID of the created folder.

        Raises:
            Exception: If the folder could not be created.
        """
        create_response = self._requests_session.post(
            f"{self._url}/api/rest/library/v2/items",
            json={
                "title": title,
                "type": ItemType.FOLDER,
                "parentId": parent_id,
                "description": description,
            },
        )

        if create_response.status_code != 201:
            raise Exception(
                f"Failed to create folder '{title}': {create_response.status_code} - {create_response.text}"
            )

        return create_response.json()["id"]

    def _get_or_create_folder(self, path: str) -> str:
        """
        Gets the folder ID for the given path, creating the folder and any necessary parent folders if they don't exist.

        Args:
            path (str): The path of the folder.

        Returns:
            str: The ID of the folder.

        Raises:
            ItemNotFoundError: If the root folder is not found or cannot be created.
        """
        parts = path.strip("/").split("/")
        current_path = ""
        folder_id: str | None = None

        for part in parts:
            current_path = f"{current_path}/{part}"

            try:
                folder_id = self._get_folder_id(current_path)
                logger.info(
                    "Folder '%s' already exists with ID: %s", current_path, folder_id
                )
            except ItemNotFoundError:
                logger.info("Folder '%s' not found. Creating it...", current_path)
                # Create the folder if it doesn't exist
                parent_id = folder_id if folder_id else self._get_folder_id("/")

                folder_id = self._create_folder(
                    title=part,
                    parent_id=parent_id,
                    description=f"Created by the Spotfire client for path '{current_path}'.",
                )

        if folder_id is None:
            # If the folder ID is still None, it means the root folder was not found
            raise ItemNotFoundError(f"Error occurred: {path}")

        return folder_id

    def _create_upload_job(
        self,
        title: str,
        item_type: ItemType,
        parent_id: str,
        description: str,
        overwrite: bool,
    ) -> str:
        """
        Creates an upload job for the given item.

        Args:
            title (str): The title of the item.
            item_type (ItemType): The type of the item.
            parent_id (str): The ID of the parent folder.
            description (str): The description of the item.
            overwrite (bool): Whether to overwrite existing items.

        Returns:
            str: The ID of the created upload job.

        Raises:
            Exception: If the upload job could not be created.
        """
        create_response = self._requests_session.post(
            f"{self._url}/api/rest/library/v2/upload",
            json={
                "overwriteIfExists": overwrite,
                "item": {
                    "title": title,
                    "type": item_type,
                    "parentId": parent_id,
                    "description": description,
                },
            },
        )

        if create_response.status_code != 201:
            raise Exception(
                f"Failed to create upload job: {create_response.status_code} - {create_response.text}"
            )

        return create_response.json()["jobId"]

    def _add_data_to_upload_job(
        self,
        data: bytes,
        job_id: str,
    ) -> str:
        """
        Uploads a file to the Spotfire library as part of an upload job.

        Args:
            data (bytes): The file data to upload.
            job_id (str): The ID of the upload job.

        Returns:
            str: The ID of the uploaded file.

        Raises:
            Exception: If the upload fails.
        """
        upload_response = self._requests_session.post(
            f"{self._url}/api/rest/library/v2/upload/{job_id}",
            data=data,
            params={
                "chunk": 1,
                "finish": True,
            },
            headers={"Content-Type": "application/octet-stream"},
        )

        if upload_response.status_code != 200:
            raise Exception(
                f"Failed to upload file: {upload_response.status_code} - {upload_response.text}"
            )

        return upload_response.json()["item"]["id"]

    def _delete_item_by_id(self, item_id: str) -> None:
        """
        Low-level delete by item ID.

        Args:
            item_id (str): The ID of the item to delete.

        Raises:
            ItemNotFoundError: If the item is not found (404).
            Exception: If the item could not be deleted.
        """
        delete_response = self._requests_session.delete(
            f"{self._url}/api/rest/library/v2/items/{item_id}",
        )

        if delete_response.status_code == 404:
            raise ItemNotFoundError(f"Item not found: {item_id}")
        if delete_response.status_code != 204:
            raise Exception(
                f"Failed to delete item: {delete_response.status_code} - {delete_response.text}"
            )

    def upload_file(
        self,
        data: bytes,
        path: str,
        item_type: ItemType,
        *,
        description: str = "",
        overwrite: bool = False,
    ) -> str:
        """
        Uploads a file to the Spotfire library.

        Args:
            data (bytes): The file data to upload.
            path (str): The path in the library where the file will be uploaded.
            item_type (ItemType): The type of the item.
            description (str, optional): The description of the item.
            overwrite (bool, optional): Whether to overwrite existing items. Defaults to False.

        Returns:
            str: The ID of the uploaded file.

        Raises:
            Exception: If the upload fails.
        """
        path_parts = path.strip("/").split("/")

        parent_folder_path = "/".join(path_parts[:-1])
        parent_id = self._get_or_create_folder(parent_folder_path)

        job_id = self._create_upload_job(
            title=path_parts[-1],
            item_type=item_type,
            parent_id=parent_id,
            description=description,
            overwrite=overwrite,
        )
        logger.info("Upload job created with ID: %s", job_id)

        file_id = self._add_data_to_upload_job(data, job_id)
        logger.info("File uploaded to %s with ID: %s", path, file_id)
        return file_id

    def delete_folder(
        self,
        path: str,
        *,
        ignore_missing: bool = True,
    ) -> None:
        """
        Deletes a folder from the Spotfire library by path.

        Args:
            path (str): The path of the folder to delete.
            ignore_missing (bool): If True, do nothing when the folder doesn't exist.

        Raises:
            ItemNotFoundError: If the folder is not found and ignore_missing is False.
            Exception: If the delete request fails for other reasons.
        """
        try:
            folder_id = self._get_folder_id(path)
        except ItemNotFoundError:
            if ignore_missing:
                logger.info("Folder '%s' not found. No action taken.", path)
                return
            raise ItemNotFoundError(message="Folder not found")

        self._delete_item_by_id(folder_id)
        logger.info("Folder '%s' deleted successfully.", path)


__all__ = [
    "LibraryClient",
]
