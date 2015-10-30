'''
Created on 26 Oct 2015

@author: Ruben.Alonso
'''
import H_Logging_Handler as LH


class Error(Exception):

    """Base class for exceptions in this module."""
    pass


class InputError(Error):
    """Exception raised for errors in the input.

    Attributes:
        expression -- input expression in which the error occurred
        message -- explanation of the error
    """

    def __init__(self, message="Generic exception", error=""):
        LH.fileLogger.info(message + error)
        self.error = error
        self.message = message


class IncorrectFormatError(Error):
    """Exception raised for errors in the input.

    Attributes:
        expression -- input expression in which the error occurred
        message -- explanation of the error
    """

    def __init__(self, message="Generic exception", error=""):
        LH.fileLogger.info(message + error)
        self.error = error
        self.message = message


class DBConnectionError(Error):
    """Exception raised for errors in the input.

    Attributes:
        expression -- input expression in which the error occurred
        message -- explanation of the error
    """

    def __init__(self, message="Generic exception", error=""):
        LH.fileLogger.info(message + error)
        self.error = error
        self.message = message
