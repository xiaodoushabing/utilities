import os
import warnings
from typing import Callable, Any
from  functools import wraps

try:
    from tenacity import Retrying, stop_after_attempt, wait_fixed, retry_any
except ImportError:
    warnings.warn("tenacity is not installed. Retry functionality will not be available."
                  " Install it with 'pip3 install tenacity' to enable retry logic.")
    Retrying = None
    stop_after_attempt = None
    wait_fixed = None
    retry_any = None

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

def _resolve(
        instance: Any | None,
        attr_name: str | None,
        explicit: Any | None,
        default: Any,
) -> Any:
    """Return a value using the precedence order: explicit > instance attribute > default.

    Args:
        instance (Any | None):
            The instance to check for the attribute (normally Self).
            If None, the instance attribute check is skipped.
        attr_name (str | None):
            The name of the attribute to look for in the instance.
            If None, the instance attribute check is skipped.
        explicit (Any | None):
            The value that was explicitly provided to the decorator.
        default (Any):
            The default value to return if neither explicit nor instance attribute is provided.
    
    Returns:
        Any: The resolved value based on the precedence order.
    """
    if explicit is not None:
        return explicit
    if instance is not None and attr_name is not None:
        return getattr(instance, attr_name, default)
    return default

def retry_args(
        func=None,
        *,
        max_attempts: int | None = None,
        wait_seconds: int | None = None,
        max_attempts_attr: str = "retry_max_attempts",
        wait_attr: str = "retry_wait",
        retry_conditions: list[Callable] | Callable | None = None,
        before_retry: Callable | None = None,
        after_retry: Callable | None = None,
        attempts_default: int = 2,
        wait_default: int = 1,
) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
    """
    Decorator to apply retry logic to a function with flexible retry conditions.
    Can be used as @retry_args or @retry_args(max_attempts=3, wait_seconds=3).
    
    Args:
        func: The function to decorate (when used without parentheses).
        max_attempts: Maximum number of retry attempts (overrides instance attribute).
        wait_seconds: Seconds to wait between retries (overrides instance attribute).
        max_attempts_attr: Name of instance attribute for max_attempts (default: "retry_max_attempts").
        wait_attr: Name of instance attribute for wait_seconds (default: "retry_wait").
        retry_conditions: Single tenacity retry condition or list of conditions.
            Pass any tenacity retry predicate(s) directly:
            - Single: retry_if_result(lambda x: x is None)
            - Multiple: [retry_if_exception_type(ConnectionError), retry_if_result(lambda x: not x)]
            If None, retries on all exceptions (default tenacity behavior).
        before_retry: Callback function called before each retry attempt.
            Receives retry_state object with: attempt_number, outcome, args, kwargs, etc.
            Example: lambda retry_state: print(f"Retry attempt {retry_state.attempt_number}")
        after_retry: Callback function called after each retry attempt.
            Receives retry_state object with: attempt_number, outcome, args, kwargs, etc.
        attempts_default: Default max_attempts if not specified elsewhere.
        wait_default: Default wait_seconds if not specified elsewhere.
    
    Returns:
        Callable: The decorated function with retry logic.
    
    Examples:
        # Log retry attempts
        @retry_args(
            retry_conditions=retry_if_exception_type(ConnectionError),
            before_retry=lambda s: print(f"Retry {s.attempt_number}/{s.retry_object.stop.max_attempt_number}"),
            max_attempts=3
        )
        def fetch(): pass
        
        # Access attempt info in a custom function
        def log_retry(retry_state):
            print(f"Attempt {retry_state.attempt_number} failed")
            if retry_state.outcome.failed:
                print(f"Error: {retry_state.outcome.exception()}")
        
        @retry_args(before_retry=log_retry)
        def fetch(): pass
    """
    def decorator(inner_func: Callable[..., Any]) -> Callable[..., Any]:
        @wraps(inner_func)
        def wrapper(*args, **kwargs):
            instance = args[0] if args and hasattr(args[0], "__dict__") else None

            attempts = _resolve(
                instance=instance,
                attr_name=max_attempts_attr,
                explicit=max_attempts,
                default=attempts_default,
            )

            wait = _resolve(
                instance=instance,
                attr_name=wait_attr,
                explicit=wait_seconds,
                default=wait_default,
            )

            if attempts < 1:
                raise ValueError("max_attempts must be at least 1")
            if wait < 0:
                raise ValueError("wait_seconds cannot be negative")

            # Build retry configuration
            retry_kwargs = dict(
                stop=stop_after_attempt(attempts),
                wait=wait_fixed(wait),
                reraise=True
            )

            # Handle retry conditions
            if retry_conditions:
                if isinstance(retry_conditions, list):
                    if len(retry_conditions) == 1:
                        retry_kwargs["retry"] = retry_conditions[0]
                    else:
                        retry_kwargs["retry"] = retry_any(*retry_conditions)
                else:
                    retry_kwargs["retry"] = retry_conditions
            
            # Add callbacks if provided
            if before_retry is not None:
                retry_kwargs["before"] = before_retry
            if after_retry is not None:
                retry_kwargs["after"] = after_retry
            
            retryer = Retrying(**retry_kwargs)
            return retryer(inner_func, *args, **kwargs)
        return wrapper
    if func is not None:
        return decorator(func)
    return decorator

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