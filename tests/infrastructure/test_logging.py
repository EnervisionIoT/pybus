import logging
import os

import pytest

from pybus.infrastructure.logging import LoggerFactory, init_logger


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


def test_init_logger_derives_log_relative_path_from_logger_name(tmp_path, monkeypatch):
    """Regression: init_logger used to be called with a `log_relative_path` kwarg
    built from an f-string over a still-unresolved dependency_injector provider
    object (e.g. `f"logs/{config.provided.APPLICATION_NAME}.log"`), which
    stringifies the provider's repr, not the actual application name, producing
    an illegal filename (Windows rejects `<`/`>`) or a garbage log path
    everywhere else. init_logger now derives the path from its own
    (already-resolved-by-the-caller) logger_name argument instead."""
    monkeypatch.chdir(tmp_path)

    logger = init_logger(logger_name="myapp")

    assert isinstance(logger, logging.Logger)
    assert LoggerFactory.logger_name == "myapp"
    assert LoggerFactory.log_filename == os.path.join(str(tmp_path), "logs/myapp.log")
    assert (tmp_path / "logs").is_dir()
