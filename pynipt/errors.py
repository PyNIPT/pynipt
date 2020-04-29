# pynipt specific error message
import warnings as wrn
from shleeh.errors import *


class InvalidLoggingLevel(Error):
    def __init__(self, level, message=None):
        if message is None:
            self.message = 'The level [{}] is invalid for logging.'.format(level)
        else:
            self.message = message


class NoneLabel(Error):
    def __init__(self, message=None):
        if message is None:
            self.message = 'None object cannot be set to label.'
        else:
            self.message = message


class InvalidPlugin(Error):
    """ Raise when the user try invalid approach """
    def __init__(self, message=None):
        if message is None:
            self.message = "Invalid plugin."
        else:
            self.message = message


class ConflictPlugin(Error):
    """ Raise when the user try invalid approach """
    def __init__(self, message=None):
        if message is None:
            self.message = "Invalid plugin."
        else:
            self.message = message