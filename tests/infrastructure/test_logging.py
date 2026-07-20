import logging
import os

import pytest

from pybus.infrastructure.logging import LoggerFactory


@pytest.fixture(autouse=True)
def reset_logger_factory_state():
    """LoggerFactory holds process-wide class state; reset it around each test
    so tests don't leak configuration/singleton state into each other."""
    original_configured = LoggerFactory._configured
    original_logger = LoggerFactory._logger
    LoggerFactory._configured = False
    LoggerFactory._logger = None
    try:
        yield
    finally:
        LoggerFactory._configured = original_configured
        LoggerFactory._logger = original_logger


def test_configure_sets_class_state_and_creates_log_directory(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)

    LoggerFactory.configure(logger_name="myapp", log_relative_path="logs/myapp.log")

    assert LoggerFactory._configured is True
    assert LoggerFactory.logger_name == "myapp"
    assert LoggerFactory.log_filename == os.path.join(str(tmp_path), "logs/myapp.log")
    assert (tmp_path / "logs").is_dir()


def test_create_logger_auto_configures_when_not_yet_configured(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)

    logger = LoggerFactory.create_logger()

    assert LoggerFactory._configured is True
    assert isinstance(logger, logging.Logger)
    assert logger.name == LoggerFactory.logger_name


def test_create_logger_returns_the_same_singleton_instance(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)

    first = LoggerFactory.create_logger()
    second = LoggerFactory.create_logger()

    assert first is second
