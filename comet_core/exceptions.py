"""Exceptions used in Comet."""


class CometBaseException(Exception):
    """Base Class for Comet Exceptions."""


class CometAlertException(CometBaseException):
    """Exception raised for errors related to Comet alerts."""

    def __init__(self, message, drop=False):
        """Create a CometAlertException.

        Args:
            message (str): Exception message
            drop (bool): Drop the alert on exception
        """
        super().__init__(message)
        self.drop = drop


class CometCouldNotSendException(CometBaseException):
    """Exception raised for when alerts could not be send."""
