import pytest
from utils.safe_run import safe_run

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
    """Dummy function that always returns True

    Returns:
        True (bool)
    """
    return True

@pytest.mark.unit
def test_retries_less_than_or_equal_to_zero():
    """Tests exception handling when retries are less than or equal to zero

    Given:
        - The safe_run decorator factory wrapping the dummy function

    When:
        - The safe_run decorator factory 'retries' parameter is less than or equal to zero

    Then:
        - A ValueError is raised
    """
    with pytest.raises(ValueError, match="'retries' has to be a positive integer"):
        safe_run(0, 10)(dummy)

@pytest.mark.unit
def test_delay_less_than_or_equal_to_zero():
    """Tests exception handling when delay less than or equal to zero

    Given:
        - The safe_run decorator factory wrapping the dummy function

    When:
        - The safe_run decorator factory 'delay' parameter is less than or equal to zero

    Then:
        - A ValueError is raised
    """
    with pytest.raises(ValueError, match="'delay' has to be a positive integer"):
        safe_run(3, 0)(dummy)

@pytest.mark.unit
def test_retries_incorrect_type():
    """Tests exception handling when retries are of an incorrect type

    Given:
        - The safe_run decorator factory wrapping the dummy function

    When:
        - The safe_run decorator factory 'retries' parameter is of type str

    Then:
        - A TypeError is raised
    """
    with pytest.raises(TypeError, match="The decorator parameters must be integers"):
        safe_run("three", 10)(dummy)

@pytest.mark.unit
def test_delay_incorrect_type():
    """Tests exception handling when delay is of an incorrect type

    Given:
        - The safe_run decorator factory wrapping the dummy function

    When:
        - The safe_run decorator factory 'delay' parameter is of type str

    Then:
        - A TypeError is raised
    """
    with pytest.raises(TypeError, match="The decorator parameters must be integers"):
        safe_run(3, "ten")(dummy)


@safe_run()
def success_func():
    """Success function wrapped with the safe_run decorator factory and always returns True

    Returns:
        True (bool)
    """
    return True


@safe_run()
def failing_func():
    """Failing function wrapped with the safe_run decorator factory and always raises an Exception

    Raises:
        Exception: An exception is always raised
    """
    raise Exception("An exception has been raised")


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

@pytest.mark.unit
def test_safe_run_succeeds():
    """Tests the success function wrapped with safe_run returns True

    Given:
        - success_func()
        - a decorator factory safe_run()

    When:
        - success_func() is called while being wrapped with safe_run()

    Then:
        - Returns True
    """
    assert success_func()

@pytest.mark.unit
def test_safe_run_fails():
    """Tests the failing function wrapped with safe_run raises an exception after the number of retries

    Given:
        - failing_func()
        - a decorator factory safe_run()

    When:
        - success_func() is called while being wrapped with safe_run()

    Then:
        - Raises Exception
    """
    with pytest.raises(Exception, match="An exception has been raised"):
        failing_func()

@pytest.mark.unit
def test_safe_run_fails_once():
    """Tests the fails once wrapped with safe_run raises returns True on the second try

    Given:
        - fails_once_func()
        - a decorator factory safe_run()
        - An attempt tracker set to count = 0

    When:
        - fails_once_func() is called while being wrapped with safe_run()

    Then:
        - Returns True
        - The attempt tracker is on count = 1
    """
    attempt_tracker["count"] = 0
    assert fails_once()
    assert attempt_tracker["count"] == 1


"""The following tests have the same test plan as the first half but have the safe_run parameters updated"""


@safe_run(5, 5)
def success_func_updated():
    return True


@safe_run(5, 5)
def failing_func_updated():
    raise Exception("An exception has been raised")


@safe_run(5, 5)
def fails_once_updated():
    if attempt_tracker["count"] == 0:
        attempt_tracker["count"] += 1
        raise Exception("An exception has been raised")
    return True

@pytest.mark.unit
def test_safe_run_succeeds_after_parameter_update():
    assert success_func_updated()

@pytest.mark.unit
def test_safe_run_fails_after_parameter_update():
    with pytest.raises(Exception, match="An exception has been raised"):
        failing_func_updated()

@pytest.mark.unit
def test_safe_run_fails_once_after_parameter_update():
    attempt_tracker["count"] = 0
    assert fails_once_updated()
    assert attempt_tracker["count"] == 1
