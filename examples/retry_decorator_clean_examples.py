"""
Examples demonstrating the clean, extensible retry_args decorator.

This approach lets users pass tenacity predicates directly, making it infinitely extensible
without changing the decorator code.
"""

import sys
import os

# Add src to path to import retry_args
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src', 'main'))

from _aux._aux import retry_args

# Import tenacity predicates you want to use
from tenacity import (
    retry_if_result,
    retry_if_exception_type,
    retry_if_exception,
    retry_if_not_result,
    retry_if_exception_message,
)


# ============================================================================
# Example 1: Simple Retry (No Conditions = Retry on Any Exception)
# ============================================================================

print("=" * 70)
print("Example 1: Simple Retry - Default Behavior")
print("=" * 70)

attempt_count_1 = 0

@retry_args(max_attempts=3, wait_seconds=1)
def simple_retry():
    """No retry conditions specified = retries on ANY exception."""
    global attempt_count_1
    attempt_count_1 += 1
    print(f"  Attempt {attempt_count_1}")
    
    if attempt_count_1 < 3:
        raise RuntimeError("Random error")
    return "Success after generic retry!"

try:
    result = simple_retry()
    print(f"  Result: {result}\n")
    attempt_count_1 = 0
except Exception as e:
    print(f"  Failed: {e}\n")


# ============================================================================
# Example 2: Retry on Specific Exception Type
# ============================================================================

print("=" * 70)
print("Example 2: Retry Only on ConnectionError")
print("=" * 70)

attempt_count_2 = 0

@retry_args(
    retry_conditions=retry_if_exception_type(ConnectionError),
    max_attempts=3,
    wait_seconds=1
)
def fetch_with_connection_error():
    """Retries ONLY on ConnectionError, not other exceptions."""
    global attempt_count_2
    attempt_count_2 += 1
    print(f"  Attempt {attempt_count_2}")
    
    if attempt_count_2 < 3:
        raise ConnectionError("Network unavailable")
    return "Success!"

try:
    result = fetch_with_connection_error()
    print(f"  Result: {result}\n")
    attempt_count_2 = 0
except Exception as e:
    print(f"  Failed: {e}\n")


# ============================================================================
# Example 3: Retry on Multiple Exception Types
# ============================================================================

print("=" * 70)
print("Example 3: Retry on Multiple Exception Types")
print("=" * 70)

attempt_count_3 = 0

@retry_args(
    retry_conditions=retry_if_exception_type((ConnectionError, TimeoutError, ValueError)),
    max_attempts=4,
    wait_seconds=1
)
def fetch_with_various_errors():
    """Retries on ConnectionError, TimeoutError, OR ValueError."""
    global attempt_count_3
    attempt_count_3 += 1
    print(f"  Attempt {attempt_count_3}")
    
    # Simulate different error types
    if attempt_count_3 == 1:
        raise ConnectionError("Connection failed")
    elif attempt_count_3 == 2:
        raise TimeoutError("Request timed out")
    elif attempt_count_3 == 3:
        raise ValueError("Invalid response")
    return "Success after multiple error types!"

try:
    result = fetch_with_various_errors()
    print(f"  Result: {result}\n")
    attempt_count_3 = 0
except Exception as e:
    print(f"  Failed: {e}\n")


# ============================================================================
# Example 4: Retry Based on Result Value
# ============================================================================

print("=" * 70)
print("Example 4: Retry if Result is Empty List")
print("=" * 70)

attempt_count_4 = 0

@retry_args(
    retry_conditions=retry_if_result(lambda x: x == []),
    max_attempts=3,
    wait_seconds=1
)
def fetch_items():
    """Retries if the result is an empty list."""
    global attempt_count_4
    attempt_count_4 += 1
    print(f"  Attempt {attempt_count_4}")
    
    if attempt_count_4 < 3:
        return []  # Empty result triggers retry
    return ["item1", "item2", "item3"]

result = fetch_items()
print(f"  Result: {result}\n")
attempt_count_4 = 0


# ============================================================================
# Example 5: Retry on Falsy Results
# ============================================================================

print("=" * 70)
print("Example 5: Retry if Result is None, Empty, or Falsy")
print("=" * 70)

attempt_count_5 = 0

@retry_args(
    retry_conditions=retry_if_result(lambda x: not x),
    max_attempts=5,
    wait_seconds=1
)
def fetch_data():
    """Retries if result is None, empty, or any falsy value."""
    global attempt_count_5
    attempt_count_5 += 1
    print(f"  Attempt {attempt_count_5}")
    
    if attempt_count_5 == 1:
        return None
    elif attempt_count_5 == 2:
        return ""
    elif attempt_count_5 == 3:
        return []
    elif attempt_count_5 == 4:
        return 0
    return {"data": "valid"}

result = fetch_data()
print(f"  Result: {result}\n")
attempt_count_5 = 0


# ============================================================================
# Example 6: Combine Multiple Conditions (OR Logic)
# ============================================================================

print("=" * 70)
print("Example 6: Retry on Exception OR Bad Result")
print("=" * 70)

attempt_count_6 = 0

@retry_args(
    retry_conditions=[
        retry_if_exception_type(ConnectionError),
        retry_if_result(lambda x: x is None)
    ],
    max_attempts=5,
    wait_seconds=1
)
def robust_api_call():
    """Retries if ConnectionError raised OR result is None."""
    global attempt_count_6
    attempt_count_6 += 1
    print(f"  Attempt {attempt_count_6}")
    
    if attempt_count_6 == 1:
        raise ConnectionError("Network error")
    elif attempt_count_6 == 2:
        return None  # Bad result triggers retry
    elif attempt_count_6 == 3:
        raise ConnectionError("Another network error")
    elif attempt_count_6 == 4:
        return None  # Another bad result
    return {"status": "ok", "data": [1, 2, 3]}

try:
    result = robust_api_call()
    print(f"  Result: {result}\n")
    attempt_count_6 = 0
except Exception as e:
    print(f"  Failed: {e}\n")


# ============================================================================
# Example 7: Custom Exception Checker with retry_if_exception
# ============================================================================

print("=" * 70)
print("Example 7: Custom Exception Logic")
print("=" * 70)

attempt_count_7 = 0

def is_temporary_error(exc):
    """Custom logic to identify temporary/retryable errors."""
    message = str(exc).lower()
    return "temporary" in message or "retry" in message or "503" in message

@retry_args(
    retry_conditions=retry_if_exception(is_temporary_error),
    max_attempts=4,
    wait_seconds=1
)
def api_with_custom_error_handling():
    """Uses custom logic to decide which errors are retryable."""
    global attempt_count_7
    attempt_count_7 += 1
    print(f"  Attempt {attempt_count_7}")
    
    if attempt_count_7 == 1:
        raise RuntimeError("Temporary database error")  # Should retry
    elif attempt_count_7 == 2:
        raise RuntimeError("503 Service Unavailable - Please retry later")  # Should retry
    elif attempt_count_7 == 3:
        # This would NOT retry (no matching keywords)
        # raise RuntimeError("Fatal error")
        pass
    return "Success with custom error handling!"

try:
    result = api_with_custom_error_handling()
    print(f"  Result: {result}\n")
    attempt_count_7 = 0
except Exception as e:
    print(f"  Failed: {e}\n")


# ============================================================================
# Example 8: retry_if_exception_message - Match Exception Message
# ============================================================================

print("=" * 70)
print("Example 8: Retry Based on Exception Message Pattern")
print("=" * 70)

attempt_count_8 = 0

@retry_args(
    retry_conditions=retry_if_exception_message(match=".*timeout.*"),
    max_attempts=3,
    wait_seconds=1
)
def api_with_timeout():
    """Retries only when exception message contains 'timeout'."""
    global attempt_count_8
    attempt_count_8 += 1
    print(f"  Attempt {attempt_count_8}")
    
    if attempt_count_8 == 1:
        raise RuntimeError("Connection timeout occurred")  # Should retry
    elif attempt_count_8 == 2:
        raise RuntimeError("Request timeout after 30 seconds")  # Should retry
    return "Success!"

try:
    result = api_with_timeout()
    print(f"  Result: {result}\n")
    attempt_count_8 = 0
except Exception as e:
    print(f"  Failed: {e}\n")


# ============================================================================
# Example 9: retry_if_not_result - Opposite Logic
# ============================================================================

print("=" * 70)
print("Example 9: Retry Until Result Meets Condition")
print("=" * 70)

attempt_count_9 = 0

@retry_args(
    retry_conditions=retry_if_not_result(lambda x: isinstance(x, dict) and "success" in x),
    max_attempts=4,
    wait_seconds=1
)
def fetch_until_success():
    """Retries until result is a dict containing 'success' key."""
    global attempt_count_9
    attempt_count_9 += 1
    print(f"  Attempt {attempt_count_9}")
    
    if attempt_count_9 == 1:
        return None  # Not a dict with 'success'
    elif attempt_count_9 == 2:
        return {"error": "not ready"}  # No 'success' key
    elif attempt_count_9 == 3:
        return ["data"]  # Not a dict
    return {"success": True, "data": [1, 2, 3]}

result = fetch_until_success()
print(f"  Result: {result}\n")
attempt_count_9 = 0


# ============================================================================
# Example 10: Complex Multi-Condition with Different Predicates
# ============================================================================

print("=" * 70)
print("Example 10: Complex Multi-Condition Retry")
print("=" * 70)

attempt_count_10 = 0

@retry_args(
    retry_conditions=[
        retry_if_exception_type((ConnectionError, TimeoutError)),
        retry_if_exception_message(match=".*503.*"),
        retry_if_result(lambda x: x is None or x == {}),
    ],
    max_attempts=6,
    wait_seconds=1
)
def comprehensive_retry():
    """
    Retries if ANY of these conditions are met:
    - ConnectionError or TimeoutError
    - Exception message contains '503'
    - Result is None or empty dict
    """
    global attempt_count_10
    attempt_count_10 += 1
    print(f"  Attempt {attempt_count_10}")
    
    if attempt_count_10 == 1:
        raise ConnectionError("Connection failed")
    elif attempt_count_10 == 2:
        return None  # Bad result
    elif attempt_count_10 == 3:
        raise RuntimeError("503 Service Unavailable")
    elif attempt_count_10 == 4:
        return {}  # Empty dict
    elif attempt_count_10 == 5:
        raise TimeoutError("Request timeout")
    return {"status": "success", "data": "final result"}

try:
    result = comprehensive_retry()
    print(f"  Result: {result}\n")
    attempt_count_10 = 0
except Exception as e:
    print(f"  Failed: {e}\n")


# ============================================================================
# Example 11: Class-Based with Instance Attributes
# ============================================================================

print("=" * 70)
print("Example 11: Class with Instance-Level Retry Configuration")
print("=" * 70)

class WeatherAPI:
    """
    API class that uses instance attributes for retry configuration.
    The decorator automatically picks up self.retry_max_attempts and self.retry_wait.
    """
    
    retry_max_attempts = 3  # Instance-level default
    retry_wait = 1
    
    def __init__(self):
        self.attempt_count = 0
    
    @retry_args(retry_conditions=retry_if_exception_type(ConnectionError))
    def get_temperature(self, city):
        """Uses instance attributes for retry configuration."""
        self.attempt_count += 1
        print(f"  Attempt {self.attempt_count} for {city}")
        
        if self.attempt_count < 3:
            raise ConnectionError("Network unavailable")
        return {"city": city, "temperature": 72}
    
    @retry_args(
        retry_conditions=retry_if_result(lambda x: x.get("temperature") is None),
        max_attempts=4  # Override instance attribute
    )
    def get_humidity(self, city):
        """Overrides instance attributes with explicit parameters."""
        self.attempt_count += 1
        print(f"  Attempt {self.attempt_count} for {city}")
        
        if self.attempt_count < 3:
            return {"city": city}  # Missing temperature triggers retry
        return {"city": city, "temperature": 68, "humidity": 75}

api = WeatherAPI()

# First method uses instance defaults (3 attempts)
result = api.get_temperature("San Francisco")
print(f"  Result: {result}")

# Reset for next test
api.attempt_count = 0

# Second method overrides with max_attempts=4
result = api.get_humidity("New York")
print(f"  Result: {result}\n")


# ============================================================================
# Example 12: Using Advanced Tenacity Features
# ============================================================================

print("=" * 70)
print("Example 12: ANY Tenacity Predicate Works!")
print("=" * 70)

# You can import and use ANY tenacity predicate
from tenacity import retry_if_result, retry_if_not_exception_type

attempt_count_12 = 0

@retry_args(
    retry_conditions=[
        # Retry on ALL exceptions EXCEPT ValueError
        retry_if_not_exception_type(ValueError),
        # Also retry if result is less than 10
        retry_if_result(lambda x: isinstance(x, int) and x < 10)
    ],
    max_attempts=5,
    wait_seconds=1
)
def advanced_retry():
    """
    Demonstrates using retry_if_not_exception_type.
    Retries on any exception except ValueError.
    """
    global attempt_count_12
    attempt_count_12 += 1
    print(f"  Attempt {attempt_count_12}")
    
    if attempt_count_12 == 1:
        raise RuntimeError("Some error")  # Retries (not ValueError)
    elif attempt_count_12 == 2:
        return 5  # Retries (result < 10)
    elif attempt_count_12 == 3:
        raise ConnectionError("Network issue")  # Retries (not ValueError)
    elif attempt_count_12 == 4:
        # This would NOT retry (ValueError is excluded)
        # raise ValueError("Bad value")
        return 8  # Retries (result < 10)
    return 15  # Success (result >= 10)

try:
    result = advanced_retry()
    print(f"  Result: {result}\n")
    attempt_count_12 = 0
except Exception as e:
    print(f"  Failed: {e}\n")


# ============================================================================
# Summary
# ============================================================================

print("=" * 70)
print("All Examples Completed Successfully!")
print("=" * 70)
print("\nKey Takeaways:")
print("1. Pass tenacity predicates directly for maximum flexibility")
print("2. Combine multiple conditions with a list (OR logic)")
print("3. Use instance attributes for class-level defaults")
print("4. Override with explicit parameters when needed")
print("5. ANY tenacity predicate works - infinitely extensible!")
print("=" * 70)
