from functools import wraps
from time import sleep

def safe_run(retries=3, delay=10):
    """Decorator factory that involves try/except and retry functionality into inputted function
    Args:
        retries (int): The maximum number of retries to carry out the function
        delay (int): The number of seconds between each retry
    """
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            for retry in range(1, retries + 1):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    # TODO: SWITCH TO LOGGER
                    print(f"This function has failed with the error: {e}")
                    print(f"This is attempt number {retry}")
                    if retry == retries:
                        raise
                    sleep(delay)
        return wrapper
    return decorator