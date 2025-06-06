import logging
import pytest
from utils.safe_run import safe_run

attempt_tracker = {"count": 0}

@safe_run()
def fails_once():
    """Returns True if the counter is not zero, otherwise it increments counter and raises Exception
    Returns:
        True (bool)
    Raises:
        Exception: An exception is always raised
    """
    if attempt_tracker["count"] == 0:
        attempt_tracker["count"] += 1
        raise Exception("An exception has been raised")
    return True

def test_logs_output_in_safe_run(caplog):
    """Tests that the logger outputs the expected logs in the safe_run function.
    Given:
        - A failing function
        - The safe_run decorator factory wrapped around the failing function
    When:
        - Calling the failing function
    Then:
        - The logger should output the expected logs in the safe_run function
    """
    caplog.set_level(logging.INFO)
    fails_once()

    assert "This function has failed with the error:" in caplog.text
    assert "This is attempt number" in caplog.text