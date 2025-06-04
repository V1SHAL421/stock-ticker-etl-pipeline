"""
__new__()
1) The first time that MainLogger is called, initialise is called
2) The second time onwards it is called, initialise is not called
3) In both cases, there is an instance

_initialise()
1) Assert log format is as expected
2) Check logger name = "Logger"
3) Check the number of handlers = 1


get_logger()
- We use caplog to capture output for
1) logging DEBUG
2) logging INFO
3) logging WARNING
4) logging ERROR
5) logging CRITICAL
6) 4) Check if timestamp is as expected
"""
import logging
import pytest
from unittest.mock import patch
from src.utils.main_logger import MainLogger

# Set up test logger
@pytest.fixture
def setup():
    test_logger = MainLogger()
    yield test_logger

# Test Cases
"""
__new__()
"""
def test_initialise_is_called():
    with patch.object(MainLogger, MainLogger._initialise()) as mock_initialise:
        _ = MainLogger()
        mock_initialise.assert_called_once()

def test_initialise_is_not_called_second_time(setup):
    with patch.object(MainLogger, MainLogger._initialise()) as mock_initialise:
        _ = MainLogger()
        mock_initialise.assert_not_called()

def test_instance_exists(setup):
    assert setup._instance != None

def test_call_second_instance(setup):
    second_logger = MainLogger()
    assert second_logger._instance != None

"""
_initialise()
"""
def test_logger_name(setup):
    expected_logger_name = "Logger"
    logger = setup.get_logger()
    assert logger.name == expected_logger_name

def test_num_handlers(setup):
    expected_num_handlers = 1
    logger = setup.get_logger()
    assert len(logger.handlers) == expected_num_handlers

"""
Output logs
"""

def test_debug_logs_does_not_output(caplog, setup):
    caplog.set_level(logging.INFO)
    logger = setup.get_logger()
    logger.debug("This is a test debug message")
    
    assert "test debug" not in caplog.text

def test_info_logs(caplog, setup):
    caplog.set_level(logging.INFO)
    logger = setup.get_logger()
    logger.info("This is a test info message")
    
    assert "test info" in caplog.text

def test_warning_logs(caplog, setup):
    caplog.set_level(logging.INFO)
    logger = setup.get_logger()
    logger.info("This is a test warning message")
    
    assert "test warning" in caplog.text

def test_error_logs(caplog, setup):
    caplog.set_level(logging.INFO)
    logger = setup.get_logger()
    logger.info("This is a test error message")
    
    assert "test error" in caplog.text


def test_critical_logs(caplog, setup):
    caplog.set_level(logging.INFO)
    logger = setup.get_logger()
    logger.info("This is a test critical message")
    
    assert "test critical" in caplog.text


