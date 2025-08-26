class DataConnectionNotFoundError(Exception):
    """
    Exception raised when a data connection is not found in the DataAccessPlan.xml.

    Args:
        message (str): Description of the error.
    """

    def __init__(self, message: str):
        super().__init__(message)


class InvalidDxpRootPathError(Exception):
    """
    Exception raised when an invalid DXP root path is provided.

    Args:
        message (str): Description of the error.
    """

    def __init__(self, message: str):
        super().__init__(message)


__all__ = [
    "DataConnectionNotFoundError",
    "InvalidDxpRootPathError",
]
