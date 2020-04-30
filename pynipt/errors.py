# pynipt specific error message
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
    """ Raise when the user try invalid plugin """
    def __init__(self, message=None):
        if message is None:
            self.message = "Invalid plugin."
        else:
            self.message = message


class InvalidMode(Error):
    """ Raise when the user try invalid mode """
    def __init__(self, message=None):
        if message is None:
            self.message = "Invalid mode."
        else:
            self.message = message


class InvalidFilter(Error):
    """ Raise when the user try invalid filter """
    def __init__(self, message=None):
        if message is None:
            self.message = "Invalid filter."
        else:
            self.message = message


class InvalidInputArg(Error):
    """ Raise when the user try invalid input argument """
    def __init__(self, message=None):
        if message is None:
            self.message = "Invalid input argument."
        else:
            self.message = message


class InvalidStepCode(Error):
    """ Raise when the user try invalid step core """
    def __init__(self, message=None):
        if message is None:
            self.message = "Invalid step code."
        else:
            self.message = message


class InvalidLabel(Error):
    """ Raise when the user try invalid package name """
    def __init__(self, message=None):
        if message is None:
            self.message = "Invalid label."
        else:
            self.message = message


class NoFunction(Error):
    """ Raise when no function found """
    def __init__(self, message=None):
        if message is None:
            self.message = "No function found for execution."
        else:
            self.message = message


class NoCommand(Error):
    """ Raise when no command found """
    def __init__(self, message=None):
        if message is None:
            self.message = "No shell command found for execution."
        else:
            self.message = message


class ConflictPlugin(Error):
    """ Raise when the plugin has confliction with other plugins """
    def __init__(self, message=None):
        if message is None:
            self.message = "Invalid plugin."
        else:
            self.message = message


class InspectionFailure(Error):
    """ Raise when the inspection failed """
    def __init__(self, message=None):
        if message is None:
            self.message = "Inspection failed."
        else:
            self.message = message


class Duplicated(Error):
    """ Raise when the duplication found """
    def __init__(self, message=None):
        if message is None:
            self.message = "Duplication found."
        else:
            self.message = message


class ErrorInThread(Error):
    """ Raise when the any exception occurred in thread"""
    def __init__(self, message=None):
        if message is None:
            self.message = "Error detected in running thread."
        else:
            self.message = message