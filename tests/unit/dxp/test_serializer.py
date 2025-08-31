import os
import tempfile
import zipfile

import pytest

from spotfire_community.dxp.serializer import (
    get_or_create_dxp_root_path,
    get_root_path_from_dxp_file,
    is_valid_dxp_folder,
    unzip_to_path,
    zip_in_memory,
    remove_temporary_files,
    get_all_files,
)
from spotfire_community.dxp.errors import InvalidDxpRootPathError


def _make_valid_dxp_folder() -> str:
    root = tempfile.mkdtemp(prefix="dxp_test_")
    # Minimal required file for is_valid_dxp_folder
    with open(os.path.join(root, "AnalysisDocument.xml"), "w", encoding="utf-8") as f:
        f.write("<AnalysisDocument />")
    # Optional data folder
    os.makedirs(os.path.join(root, "DataArchive"), exist_ok=True)
    return root


def _make_dxp_file_from_folder(folder: str) -> str:
    fd, path = tempfile.mkstemp(suffix=".dxp", prefix="dxp_zip_")
    os.close(fd)
    with zipfile.ZipFile(path, "w", zipfile.ZIP_STORED) as z:
        for root, _, files in os.walk(folder):
            for name in files:
                full = os.path.join(root, name)
                arc = os.path.relpath(full, start=folder)
                z.write(full, arc)
    return path


def test_is_valid_dxp_folder_true_and_false():
    valid = _make_valid_dxp_folder()
    try:
        assert is_valid_dxp_folder(valid) is True
        invalid = tempfile.mkdtemp(prefix="dxp_invalid_")
        try:
            assert is_valid_dxp_folder(invalid) is False
        finally:
            remove_temporary_files(invalid)
    finally:
        remove_temporary_files(valid)


def test_get_or_create_dxp_root_path_with_folder_and_file():
    folder = _make_valid_dxp_folder()
    try:
        res = get_or_create_dxp_root_path(folder)
        assert res.dxp_root_path == folder and res.cleanup_required is False

        dxp_file = _make_dxp_file_from_folder(folder)
        try:
            res2 = get_or_create_dxp_root_path(dxp_file)
            assert os.path.isdir(res2.dxp_root_path)
            assert res2.cleanup_required is True
            # cleanup created temp dir
            remove_temporary_files(res2.dxp_root_path)
        finally:
            os.remove(dxp_file)
    finally:
        remove_temporary_files(folder)


def test_get_or_create_dxp_root_path_invalid_raises():
    with pytest.raises(InvalidDxpRootPathError):
        get_or_create_dxp_root_path("/path/that/does/not/exist")


def test_get_root_path_from_dxp_file_errors_and_success():
    # Not a .dxp file
    with pytest.raises(ValueError):
        get_root_path_from_dxp_file("/tmp/not_a_dxp.txt")

    # A .dxp missing AnalysisDocument.xml should raise InvalidDxpRootPathError
    fd, bad_path = tempfile.mkstemp(suffix=".dxp", prefix="dxp_bad_")
    os.close(fd)
    try:
        with zipfile.ZipFile(bad_path, "w") as z:
            z.writestr("something_else.txt", "oops")
        with pytest.raises(InvalidDxpRootPathError):
            get_root_path_from_dxp_file(bad_path)
    finally:
        os.remove(bad_path)

    # Valid .dxp
    folder = _make_valid_dxp_folder()
    try:
        dxp_file = _make_dxp_file_from_folder(folder)
        extracted: str | None = None
        try:
            extracted = get_root_path_from_dxp_file(dxp_file)
            assert os.path.isdir(extracted)
        finally:
            os.remove(dxp_file)
            if extracted:
                remove_temporary_files(extracted)
    finally:
        remove_temporary_files(folder)


def test_unzip_to_path_and_get_all_files_and_zip_in_memory():
    src = _make_valid_dxp_folder()
    try:
        # nested content
        os.makedirs(os.path.join(src, "Subdir"), exist_ok=True)
        with open(os.path.join(src, "Subdir", "file.txt"), "w", encoding="utf-8") as f:
            f.write("hello")

        dst = tempfile.mkdtemp(prefix="dxp_out_")
        try:
            unzip_to_path(src, dst)
            files = get_all_files(dst)
            assert any(p.endswith("AnalysisDocument.xml") for p in files)
            assert any(p.endswith(os.path.join("Subdir", "file.txt")) for p in files)

            memzip = zip_in_memory(src)
            with zipfile.ZipFile(memzip) as z:
                names = z.namelist()
                assert "AnalysisDocument.xml" in names
                assert os.path.join("Subdir", "file.txt") in names
        finally:
            remove_temporary_files(dst)

        # invalid root path should raise
        with pytest.raises(InvalidDxpRootPathError):
            unzip_to_path("/does/not/exist", dst)
    finally:
        remove_temporary_files(src)
