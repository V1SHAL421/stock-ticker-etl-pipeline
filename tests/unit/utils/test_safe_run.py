import pytest
from src.utils.safe_run import safe_run
"""
Define a function to be decorated

Incorrect parameters:
1) retries is less than or equal to 0
2) delay is less than or equal to 0
3) retries is an incorrect format
4) delay is an incorrect format

Without changing parameters:
1) Succeeds
2) Fails once then succeeds
3) Fails everytime

With changing parameters
1) Succeeds
2) Fails once then succeeds
3) Fails everytime
"""

def dummy():
    return True

def test_retries_less_than_or_equal_to_zero():
    with pytest.raises(ValueError, match="'retries' has to be a positive integer"):
        safe_run(0, 10)(dummy)

def test_delay_less_than_or_equal_to_zero():
    with pytest.raises(ValueError, match="'delay' has to be a positive integer"):
        safe_run(3, 0)(dummy)

def test_retries_incorrect_type():
    with pytest.raises(TypeError, match="The decorator parameters must be integers"):
        safe_run("three", 10)(dummy)

def test_delay_incorrect_type():
    with pytest.raises(TypeError, match="The decorator parameters must be integers"):
        safe_run(3, "ten")(dummy)

@safe_run()
def success_func():
    return True

@safe_run()
def failing_func():
    raise Exception("An exception has been raised")

attempt_tracker = {"count": 0}

@safe_run
def fails_once():
    if attempt_tracker["count"] == 0:
        attempt_tracker["count"] += 1
        raise Exception("An exception has been raised")
    return True

def test_safe_run_succeeds():
    assert success_func() == True

def test_safe_run_fails():
    with pytest.raises(Exception, match="An exception has been raised"):
        failing_func()

def test_safe_run_fails_once():
    attempt_tracker["count"] = 0
    assert fails_once() == True
    assert attempt_tracker["count"] == 1

@safe_run(5, 5)
def success_func():
    return True

@safe_run(5, 5)
def failing_func():
    raise Exception("An exception has been raised")

@safe_run(5, 5)
def fails_once():
    if attempt_tracker["count"] == 0:
        attempt_tracker["count"] += 1
        raise Exception("An exception has been raised")
    return True

def test_safe_run_succeeds_after_parameter_update():
    assert success_func() == True

def test_safe_run_fails_after_parameter_update():
    with pytest.raises(Exception, match="An exception has been raised"):
        failing_func()

def test_safe_run_fails_once_after_parameter_update():
    attempt_tracker["count"] = 0
    assert fails_once() == True
    assert attempt_tracker["count"] == 1