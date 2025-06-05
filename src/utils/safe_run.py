from functools import wraps
from time import sleep


def safe_run(retries: int = 3, delay: int = 10):
    """Decorator factory that adds try/except and retry functionality to a function.

    Args:
        retries (int): Maximum number of retry attempts.
        delay (int): Delay in seconds between retries.

    Returns:
        Callable: A decorator that applies the retry logic.

    Raises:
        TypeError: If `retries` or `delay` is not an integer.
        ValueError: If `retries` or `delay` is not a positive integer.
    """

    if not isinstance(retries, int) or not isinstance(delay, int):
        raise TypeError("The decorator parameters must be integers")

    if retries <= 0:
        raise ValueError("'retries' has to be a positive integer")
    if delay <= 0:
        raise ValueError("'delay' has to be a positive integer")

    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            for retry in range(1, retries + 1):  # Iterate through each retry attempt
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    # TODO: SWITCH TO LOGGER
                    print(f"This function has failed with the error: {e}")
                    print(f"This is attempt number {retry}")
                    if (
                        retry == retries
                    ):  # If the maximum number of retries has been reached
                        raise
                    sleep(delay)  # Delay between retry attempts

        return wrapper

    return decorator
