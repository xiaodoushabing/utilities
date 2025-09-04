import os
import warnings
from typing import Callable
from  functools import wraps

try:
    from tenacity import Retrying, stop_after_attempt, wait_fixed
except ImportError:
    warnings.warn("tenacity is not installed. Retry functionality will not be available." /
                  " Install it with 'pip3 install tenacity' to enable retry logic.")
    Retrying = None
    stop_after_attempt = None
    wait_fixed = None

def append_to_path_var(path_var: str, path: str) -> None:
    """
    Append a directory to the system PATH variable.
    
    Args:
        path_var (str): The environment variable to modify, typically 'PATH'.
        path (str): The directory to append to the PATH.
    """
    existing_path = os.getenv(path_var, '')
    if path not in existing_path:
        os.environ[path_var] = path if not existing_path else f"{path}:{existing_path.strip(':')}"


def retry_args(func: Callable):
    """
    Decorator to apply retry logic to a function.
    """
    @wraps(func)
    def wrapper(*args, max_attempts: int = 3, wait: int = 1, **kwargs):
        """
        Wrapper function to apply retry logic.
        
        Args:
            max_attempts (int): Maximum number of retry attempts.
            wait (int): Wait time between retries in seconds.
        
        Returns:
            The result of the function call.
        """
        try:
            retryer = Retrying(
                stop=stop_after_attempt(max_attempts),
                wait=wait_fixed(wait),
                reraise=True
            )
            return retryer(func, *args, **kwargs)
        
        except Exception:
            return func(*args, **kwargs)
    return wrapper

def iter_update_dict(dt_base: dict, dt_new: dict) -> dict:
    """
    Recursively update a dictionary with another dictionary.
    
    Args:
        dt_base (dict): The original dictionary to be updated.
        dt_new (dict): The dictionary with updates.
    
    Returns:
        dict: The updated dictionary.
    """
    for k, vv in dt_new.items():
        if k not in dt_base.keys():
            dt_base[k] = vv
        else:
            v = dt_base.get(k, {})
            if isinstance (v, dict):
                # print(vv)
                dt_base[k] = iter_update_dict(v, vv)
            else:
                dt_base.update({k: vv})
    return dt_base