class ItemNotFoundError(Exception):
    """
    Exception raised when an item is not found in the Spotfire library.

    Args:
        message (str): Description of the error.
    """

    def __init__(self, message: str):
        super().__init__(message)
