"""Interface to define the contract for try except functions"""
from typing import Protocol

class TryExcept(Protocol):
    def try_except(func, retries=3):
        success = False
        for _ in retries:
            try:
                func()
                success = True
                break
            except FileNotFoundError as e:
                pass
        if not success:
            raise
        
