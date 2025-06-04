import logging
import pytest
from unittest.mock import patch
from utils.main_logger import MainLogger

@pytest.fixture(autouse=True)
def reset_main_logger():
    """Resets the MainLogger Singleton instance to None"""
    MainLogger._instance = None

# Set up test logger
@pytest.fixture
def setup():
    """Sets up the MainLogger instance"""
    test_logger = MainLogger()
    yield test_logger

# Test Cases
"""
__new__()
"""
def test_initialise_is_called():
    """Tests that _initialise() is called once upon the first time the MainLogger is called.
    
    Given:
        - A patched object of the MainLogger and a mocked initialise() method
    
    When:
        - Calling _initialise() on the instance
    
    Then:
        - The _initialise() method should only be called once
    """
    with patch.object(MainLogger, "_initialise") as mock_initialise:
        _ = MainLogger()
        mock_initialise.assert_called_once()

def test_initialise_is_not_called_second_time(setup):
    """Tests that _initialise() is not called upon the second time the MainLogger is called.
    
    Given:
        - A patched object of the MainLogger and a mocked initialise() method
    
    When:
        - Calling _initialise() on the instance
    
    Then:
        - The _initialise() method should not be called
    """
    with patch.object(MainLogger, "_initialise") as mock_initialise:
        _ = MainLogger()
        mock_initialise.assert_not_called()

def test_instance_exists(setup):
    """Tests that _instance exists upon the first time the MainLogger is called.
    
    Given:
        - An instance of the MainLogger via the fixture
    
    When:
        - Accessing the _instance attribute
    
    Then:
        - The _instance should not be None
    """
    assert setup._instance != None

def test_call_second_instance(setup):
    """Tests that _instance exists upon the second time the MainLogger is called.
    
    Given:
        - Two instances of the MainLogger, one via the fixture and one initialised
    
    When:
        - Accessing the _instance attribute
    
    Then:
        - The _instance should not be None
    """
    second_logger = MainLogger()
    assert second_logger._instance != None

"""
_initialise()
"""
def test_logger_name(setup):
    """Tests that MainLogger is initialised with the correct name.
    
    Given:
        - An instance of the MainLogger via the fixture
    
    When:
        - Calling get_logger() on the instance
    
    Then:
        - The logger name should be the expected logger name
    """
    expected_logger_name = "Logger"
    logger = setup.get_logger()
    # logger = setup.get_logger()
    assert logger.name == expected_logger_name

def test_num_handlers(setup):
    """Tests that MainLogger is initialised with the correct number of handlers.
    
    Given:
        - An instance of the MainLogger via the fixture
    
    When:
        - Calling get_logger() on the instance
    
    Then:
        - The logger should have the expected number of handlers
    """
    expected_num_handlers = 5
    logger = setup.get_logger()
    assert len(logger.handlers) == expected_num_handlers

"""
Output logs
"""

def test_debug_logs_does_not_output(caplog, setup):
    """Tests that the logger does not output any debug level logs.
    
    Given:
        - An instance of the MainLogger via the fixture
        - The logger of the instance
    
    When:
        - Calling debug() on the logger
    
    Then:
        - The logger should not output the message
    """
    caplog.set_level(logging.INFO)
    logger = setup.get_logger()
    logger.debug("This is a test debug message")
    
    assert "test debug" not in caplog.text

def test_info_logs(caplog, setup):
    """Tests that the logger outputs info level logs.
    
    Given:
        - An instance of the MainLogger via the fixture
        - The logger of the instance
    
    When:
        - Calling info() on the logger
    
    Then:
        - The logger should output the message
    """
    caplog.set_level(logging.INFO)
    logger = setup.get_logger()
    logger.info("This is a test info message")
    
    assert "test info" in caplog.text

def test_warning_logs(caplog, setup):
    """Tests that the logger outputs warning level logs.
    
    Given:
        - An instance of the MainLogger via the fixture
        - The logger of the instance
    
    When:
        - Calling warning() on the logger
    
    Then:
        - The logger should output the message
    """
    caplog.set_level(logging.INFO)
    logger = setup.get_logger()
    logger.warning("This is a test warning message")
    
    assert "test warning" in caplog.text

def test_error_logs(caplog, setup):
    """Tests that the logger outputs error level logs.
    
    Given:
        - An instance of the MainLogger via the fixture
        - The logger of the instance
    
    When:
        - Calling error() on the logger
    
    Then:
        - The logger should output the message
    """
    caplog.set_level(logging.INFO)
    logger = setup.get_logger()
    logger.error("This is a test error message")
    
    assert "test error" in caplog.text


def test_critical_logs(caplog, setup):
    """Tests that the logger outputs critical level logs.
    
    Given:
        - An instance of the MainLogger via the fixture
        - The logger of the instance
    
    When:
        - Calling critical() on the logger
    
    Then:
        - The logger should output the message
    """
    caplog.set_level(logging.INFO)
    logger = setup.get_logger()
    logger.critical("This is a test critical message")
    
    assert "test critical" in caplog.text


