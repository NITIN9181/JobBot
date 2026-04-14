import time
import logging
import functools
from typing import Callable, Any

logger = logging.getLogger(__name__)

def retry(max_attempts: int = 3, delay: int = 5):
    """
    A decorator that retries a function if it raises an exception.
    
    Args:
        max_attempts: Maximum number of attempts before giving up.
        delay: Seconds to wait between attempts.
    """
    def decorator(func: Callable):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            attempts = 0
            while attempts < max_attempts:
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    attempts += 1
                    if attempts >= max_attempts:
                        logger.error(f"Function '{func.__name__}' failed after {max_attempts} attempts: {str(e)}")
                        raise e
                    
                    logger.warning(
                        f"Attempt {attempts}/{max_attempts} for '{func.__name__}' failed: {str(e)}. "
                        f"Retrying in {delay} seconds..."
                    )
                    time.sleep(delay)
            return None # Should not be reachable
        return wrapper
    return decorator

if __name__ == "__main__":
    # Test retry decorator
    logging.basicConfig(level=logging.INFO)
    
    @retry(max_attempts=3, delay=1)
    def test_func():
        print("Executing test_func...")
        raise ValueError("Simulated failure")
        
    try:
        test_func()
    except Exception:
        print("Final failure caught.")
