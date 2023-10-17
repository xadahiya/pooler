import logging
import os

from gunicorn.app.base import BaseApplication
from gunicorn.glogging import Logger
from loguru import logger

LOG_LEVEL = logging.getLevelName(os.environ.get('LOG_LEVEL', 'DEBUG'))


class InterceptHandler(logging.Handler):
    def emit(self, record):
        """

    Emits a log record.

    Args:
        self: The instance of the class.
        record: The log record to emit.

    Returns:
        None

    This function emits a log record by converting it to the corresponding Loguru level, finding the caller from where the log message originated, and logging the message using the Loguru logger. If the corresponding Loguru level does not exist, the function uses the level number from the log record.

    The function first tries to get the corresponding Loguru level by using the level name from the log record. If the level name is not found, it uses the level number from the log record.

    Next, the function finds the caller from where the log message originated. It does this by traversing the call stack and checking if the filename of the current frame matches the filename of the logging module. If it does, it moves to the previous frame and increments the depth counter. This process continues until a frame is found that does not belong to the logging module.

    Finally, the function logs the message using the Loguru logger. It sets the depth parameter to the calculated depth and passes the exception information from the log record if available.

    Note:
        This function assumes that the Loguru logger is already configured and available as 'logger'.

    Example:
        emit(self, record)

    """
        # Get corresponding Loguru level if it exists
        try:
            level = logger.level(record.levelname).name
        except ValueError:
            level = record.levelno

        # Find caller from where originated the logged message
        frame, depth = logging.currentframe(), 2
        while frame.f_code.co_filename == logging.__file__:
            frame = frame.f_back
            depth += 1

        logger.opt(depth=depth, exception=record.exc_info).log(
            level,
            record.getMessage(),
        )


class StubbedGunicornLogger(Logger):
    def setup(self, cfg):
        """
    Sets up the logging configuration for the application.

    Args:
        cfg (object): The configuration object.

    Returns:
        None

    Raises:
        None

    Notes:
        This function sets up the error and access loggers for the application. It creates a `NullHandler` and adds it to both loggers. It also sets the log level for both loggers to the value specified by `LOG_LEVEL`.
    """
        handler = logging.NullHandler()
        self.error_logger = logging.getLogger('gunicorn.error')
        self.error_logger.addHandler(handler)
        self.access_logger = logging.getLogger('gunicorn.access')
        self.access_logger.addHandler(handler)
        self.error_log.setLevel(LOG_LEVEL)
        self.access_log.setLevel(LOG_LEVEL)


class StandaloneApplication(BaseApplication):
    """Our Gunicorn application."""

    def __init__(self, app, options=None):
        """
    Initializes a new instance of the class.

    Args:
        app: The application object.
        options: Optional dictionary of options. Defaults to None.

    Attributes:
        options: Dictionary of options.
        application: The application object.


    """
        self.options = options or {}
        self.application = app
        super().__init__()

    def load_config(self):
        """
    Loads the configuration settings into the application.

    Parameters:
    - self: The instance of the class.

    Returns:
    None

    Raises:
    None

    Example:
    ```
    load_config()
    ```
    """
        config = {
            key: value
            for key, value in self.options.items()
            if key in self.cfg.settings and value is not None
        }
        for key, value in config.items():
            self.cfg.set(key.lower(), value)

    def load(self):
        """
    Load the application.

    Returns:
        The application object.


    """
        return self.application
