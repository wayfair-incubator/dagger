class DaggerError(Exception):
    """Base-class for all Dagger exceptions."""


class InvalidTaskType(DaggerError):
    """Invalid Task Type."""


class TaskInvalidState(DaggerError):
    """Invalid Task State."""


class TemplateDoesNotExist(DaggerError):
    """Invalid Template Name"""


class InvalidTriggerTimeForTask(DaggerError):
    """Invalid trigger time"""
