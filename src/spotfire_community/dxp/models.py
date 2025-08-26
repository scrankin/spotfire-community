from typing import NamedTuple


class GetOrCreateDxpRootPathReturn(NamedTuple):
    """
    Named tuple for the result of get_or_create_dxp_root_path.

    Attributes:
        dxp_root_path (str): The path to the DXP root folder.
        cleanup_required (bool): Whether cleanup is required for the temporary folder.
    """

    dxp_root_path: str
    cleanup_required: bool


__all__ = ["GetOrCreateDxpRootPathReturn"]
