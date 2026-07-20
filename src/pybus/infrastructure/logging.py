import logging
import os
from logging.config import dictConfig
from typing import Any


class LoggerFactory:
    _configured: bool = False
    _logger: logging.Logger | None = None

    @classmethod
    def configure(cls, logger_name: str = "pybus", log_relative_path: str = "logs/pybus.log"):
        project_dir = os.getcwd()
        full_log_path = os.path.join(project_dir, log_relative_path)
        os.makedirs(os.path.dirname(full_log_path), exist_ok=True)

        cls.logger_name: str = logger_name
        cls.log_filename: str = full_log_path
        cls._configured = True

    @classmethod
    def create_logger(cls) -> logging.Logger:
        if cls._logger is not None:
            return cls._logger

        if not cls._configured:
            cls.configure()
        logging_config: dict[str, Any] = {
            "version": 1,
            "disable_existing_loggers": False,
            "formatters": {
                "default": {"format": "%(asctime)s %(name)-12s %(levelname)-8s %(message)s"},
                "colored": {
                    "()": "colorlog.ColoredFormatter",
                    "format": "%(log_color)s%(asctime)s %(white)s%(name)-12s %(log_color)s%(levelname)-8s %(blue)s%(message)s",
                    "datefmt": "%Y-%m-%d %H:%M:%S",
                },
            },
            "handlers": {
                "console": {
                    "class": "logging.StreamHandler",
                    "formatter": "default",
                },
                "colored_console": {
                    "class": "logging.StreamHandler",
                    "formatter": "colored",
                },
                "file_handler": {
                    "class": "logging.handlers.RotatingFileHandler",
                    "formatter": "default",
                    "filename": cls.log_filename,
                    "maxBytes": 10485760,
                    "backupCount": 5,
                    "encoding": "utf8",
                },
            },
            "loggers": {
                "uvicorn.access": {
                    "level": "INFO",
                    "handlers": ["console"],
                    "propagate": False,
                },
                "uvicorn.error": {
                    "level": "INFO",
                    "handlers": ["console"],
                    "propagate": False,
                },
                cls.logger_name: {
                    "level": "DEBUG",
                    "handlers": ["colored_console", "file_handler"],
                    "propagate": False,
                },
            },
        }
        dictConfig(logging_config)
        cls._logger = logging.getLogger(cls.logger_name)
        return cls._logger
