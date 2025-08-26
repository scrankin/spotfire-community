from io import BytesIO
import os
import shutil
import tempfile
import zipfile

from .errors import InvalidDxpRootPathError
from .models import GetOrCreateDxpRootPathReturn


def get_or_create_dxp_root_path(dxp_path: str) -> GetOrCreateDxpRootPathReturn:
    """
    Get or create a DXP root path from a given path.

    Args:
        dxp_path (str): Path to a DXP folder or .dxp file.

    Returns:
        GetOrCreateDxpRootPathReturn: Named tuple with dxp_root_path and cleanup_required flag.

    Raises:
        InvalidDxpRootPathError: If the path is not a valid DXP folder or file.
    """
    if is_valid_dxp_folder(dxp_path):
        return GetOrCreateDxpRootPathReturn(
            dxp_root_path=dxp_path, cleanup_required=False
        )
    elif os.path.isfile(dxp_path) and dxp_path.endswith(".dxp"):
        return GetOrCreateDxpRootPathReturn(
            dxp_root_path=get_root_path_from_dxp_file(dxp_path=dxp_path),
            cleanup_required=True,
        )
    else:
        raise InvalidDxpRootPathError(
            f"Path {dxp_path} is not a valid DXP folder or file."
        )


def get_root_path_from_dxp_file(dxp_path: str) -> str:
    """
    Extracts a .dxp file to a temporary directory and returns the root path.

    Args:
        dxp_path (str): Path to the .dxp file.

    Returns:
        str: Path to the extracted DXP root folder.

    Raises:
        ValueError: If the input is not a .dxp file.
        InvalidDxpRootPathError: If the extracted folder is not a valid DXP folder.
    """
    if not (os.path.isfile(dxp_path) and dxp_path.endswith(".dxp")):
        raise ValueError(
            "Input must be a .dxp file. If you are passing an unzipped folder, please use the constructor."
        )

    root_path = tempfile.mkdtemp(prefix="dxp_edit_")

    with zipfile.ZipFile(dxp_path, "r") as zip_ref:
        zip_ref.extractall(root_path)

    if not is_valid_dxp_folder(root_path):
        raise InvalidDxpRootPathError(
            f"Extracted folder from {dxp_path} is not a valid DXP folder. DXP may be invalid."
        )

    return root_path


def is_valid_dxp_folder(root_path: str) -> bool:
    """
    Checks if the given path is a valid DXP folder.

    Args:
        root_path (str): Path to check.

    Returns:
        bool: True if valid, False otherwise.
    """
    return os.path.isdir(root_path) and os.path.isfile(
        os.path.join(root_path, "AnalysisDocument.xml")
    )


def unzip_to_path(
    dxp_root_path: str,
    output_path: str,
):
    """
    Copies the contents of a DXP root folder to an output directory.

    Args:
        dxp_root_path (str): Path to the DXP root folder.
        output_path (str): Path to the output directory.

    Raises:
        InvalidDxpRootPathError: If the root path is not a valid DXP folder.
    """
    if not is_valid_dxp_folder(dxp_root_path):
        raise InvalidDxpRootPathError(
            f"Root path {dxp_root_path} is not a valid directory."
        )

    # Create the output directory if it doesn't exist
    os.makedirs(output_path, exist_ok=True)

    # Copy files to the output directory
    for item in os.listdir(dxp_root_path):
        source_path = os.path.join(dxp_root_path, item)
        destination_path = os.path.join(output_path, item)
        if os.path.isdir(source_path):
            shutil.copytree(source_path, destination_path, False, None)
        else:
            shutil.copy2(source_path, destination_path)


def zip_in_memory(dxp_root_path: str) -> BytesIO:
    """
    Zips the contents of a DXP root folder into an in-memory BytesIO object.

    Args:
        dxp_root_path (str): Path to the DXP root folder.

    Returns:
        BytesIO: In-memory zip file.
    """
    memory_zip = BytesIO()

    with zipfile.ZipFile(memory_zip, "w", zipfile.ZIP_STORED) as zip_file:
        for root, _, files in os.walk(dxp_root_path):
            for file in files:
                file_path = os.path.join(root, file)
                zipped_path = os.path.relpath(
                    file_path, start=dxp_root_path
                )  # To keep relative paths
                zip_file.write(file_path, zipped_path)

    memory_zip.seek(0)
    return memory_zip


def remove_temporary_files(root_path: str):
    """
    Removes a temporary directory and its contents.

    Args:
        root_path (str): Path to the directory to remove.
    """
    if os.path.isdir(root_path):
        shutil.rmtree(root_path)


def get_all_files(dxp_root_path: str) -> list[str]:
    """
    Lists all files in a directory and its subdirectories.

    Args:
        dxp_root_path (str): Path to the root directory.

    Returns:
        list[str]: List of file paths.
    """
    all_files: list[str] = []
    for root, _, files in os.walk(dxp_root_path):
        for file in files:
            all_files.append(os.path.join(root, file))
    return all_files


__all__ = [
    "unzip_to_path",
    "zip_in_memory",
    "is_valid_dxp_folder",
    "get_root_path_from_dxp_file",
    "get_or_create_dxp_root_path",
    "get_all_files",
]
