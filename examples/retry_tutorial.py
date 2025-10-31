# %% [markdown]
"""
# Complete Tutorial: retry_args Decorator

This tutorial covers all the essential patterns you need to implement robust retry logic.
Each example can be copied and adapted for your specific use case.

Run each cell independently to see the retry patterns in action.
"""

# %% [python]
# Setup and Imports
import sys
import os
import time

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src', 'main'))

from _aux._aux import retry_args
from tenacity import (
    retry_if_exception_type,
    retry_if_result,
    retry_if_exception_message,
    retry_if_not_result
)

# %% [markdown]
"""
## Basic Retry Patterns

These are the fundamental retry patterns you'll use most often.
"""

# %%

# Example 1: Simple Retry - Retries on any exception
print("Example 1: Simple Retry - Retries on any exception")
print("-" * 50)

attempt_count_1 = 0

@retry_args(max_attempts=3, wait_seconds=1)
def simple_retry():
    global attempt_count_1
    attempt_count_1 += 1
    print(f"  Basic retry - Attempt {attempt_count_1}")
    if attempt_count_1 < 3:
        raise RuntimeError("Random error")
    return "Success!"

result = simple_retry()
print(f"  Result: {result}\n")
attempt_count_1 = 0

# %%
# Example 2: Retry on Specific Exceptions
print("Example 2: Retry on Specific Exceptions")
print("-" * 50)

attempt_count_2 = 0

@retry_args(
    retry_conditions=retry_if_exception_type(ConnectionError),
    max_attempts=3,
    wait_seconds=1
)
def retry_connection_errors():
    global attempt_count_2
    attempt_count_2 += 1
    print(f"  Connection retry - Attempt {attempt_count_2}")
    if attempt_count_2 < 3:
        raise ConnectionError("Network unavailable")
    return "Connected!"

result = retry_connection_errors()
print(f"  Result: {result}\n")
attempt_count_2 = 0

# %%
# Example 3: Retry on Multiple Exception Types
print("Example 3: Retry on Multiple Exception Types")
print("-" * 50)

attempt_count_3 = 0

@retry_args(
    retry_conditions=retry_if_exception_type((ConnectionError, TimeoutError, ValueError)),
    max_attempts=4,
    wait_seconds=1
)
def retry_multiple_errors():
    global attempt_count_3
    attempt_count_3 += 1
    print(f"  Multi-error retry - Attempt {attempt_count_3}")
    
    if attempt_count_3 == 1:
        raise ConnectionError("Connection failed")
    elif attempt_count_3 == 2:
        raise TimeoutError("Request timed out")
    elif attempt_count_3 == 3:
        raise ValueError("Invalid response")
    return "Success!"

result = retry_multiple_errors()
print(f"  Result: {result}\n")
attempt_count_3 = 0

# %% [markdown]
"""
## Result-Based Retry Patterns

These patterns retry based on the return value of the function.
"""

# %%
# Example 4: Retry on Empty/None Results
print("Example 4: Retry on Empty/None Results")
print("-" * 50)

attempt_count_4 = 0

@retry_args(
    retry_conditions=retry_if_result(lambda x: x is None or x == []),
    max_attempts=3,
    wait_seconds=1
)
def retry_empty_results():
    global attempt_count_4
    attempt_count_4 += 1
    print(f"  Empty result retry - Attempt {attempt_count_4}")
    
    if attempt_count_4 == 1:
        return None
    elif attempt_count_4 == 2:
        return []
    return ["data1", "data2"]

result = retry_empty_results()
print(f"  Result: {result}\n")
attempt_count_4 = 0

# %%
# Example 5: Retry Until Success Condition
print("Example 5: Retry Until Success Condition")
print("-" * 50)

attempt_count_5 = 0

@retry_args(
    retry_conditions=retry_if_not_result(lambda x: isinstance(x, dict) and x.get("status") == "success"),
    max_attempts=4,
    wait_seconds=1
)
def retry_until_success():
    global attempt_count_5
    attempt_count_5 += 1
    print(f"  Success condition retry - Attempt {attempt_count_5}")
    
    if attempt_count_5 == 1:
        return {"status": "pending"}
    elif attempt_count_5 == 2:
        return {"status": "processing"}
    elif attempt_count_5 == 3:
        return {"error": "not ready"}
    return {"status": "success", "data": "complete"}

result = retry_until_success()
print(f"  Result: {result}\n")
attempt_count_5 = 0

# %% [markdown]
"""
## Combined Retry Conditions

You can combine multiple retry conditions using a list (OR logic).
"""

# %%
# Example 6: Multiple Conditions (OR Logic)
print("Example 6: Multiple Conditions (OR Logic)")
print("-" * 50)

attempt_count_6 = 0

@retry_args(
    retry_conditions=[
        retry_if_exception_type(ConnectionError),
        retry_if_result(lambda x: x is None),
        retry_if_exception_message(match=".*503.*")
    ],
    max_attempts=5,
    wait_seconds=1
)
def comprehensive_retry():
    """Retries if: ConnectionError OR result is None OR message contains '503'"""
    global attempt_count_6
    attempt_count_6 += 1
    print(f"  Combined conditions - Attempt {attempt_count_6}")
    
    if attempt_count_6 == 1:
        raise ConnectionError("Network error")
    elif attempt_count_6 == 2:
        return None
    elif attempt_count_6 == 3:
        raise RuntimeError("503 Service Unavailable")
    elif attempt_count_6 == 4:
        return None
    return {"status": "success"}

result = comprehensive_retry()
print(f"  Result: {result}\n")
attempt_count_6 = 0

# %% [markdown]
"""
## Logging and Monitoring Patterns

These patterns help you monitor retry behavior and debug issues.
"""

# %%
# Example 7: Basic Attempt Logging
print("Example 7: Basic Attempt Logging")
print("-" * 50)

attempt_count_7 = 0

@retry_args(
    max_attempts=3,
    wait_seconds=1,
    before_retry=lambda s: print(f"  🔄 About to retry attempt {s.attempt_number}")
)
def basic_logging():
    global attempt_count_7
    attempt_count_7 += 1
    if attempt_count_7 < 3:
        raise RuntimeError("Still failing")
    return "Success!"

result = basic_logging()
print(f"  Result: {result}\n")
attempt_count_7 = 0

# %%
# Example 8: Detailed Error Logging
print("Example 8: Detailed Error Logging")
print("-" * 50)

attempt_count_8 = 0

def log_retry_details(retry_state):
    """Log detailed information after each failed attempt"""
    attempt = retry_state.attempt_number
    max_attempts = retry_state.retry_object.stop.max_attempt_number
    print(f"  ❌ Attempt {attempt}/{max_attempts} failed")
    
    if retry_state.outcome is not None and retry_state.outcome.failed:
        exc = retry_state.outcome.exception()
        print(f"     Error: {type(exc).__name__}: {exc}")

@retry_args(
    retry_conditions=retry_if_exception_type((ConnectionError, TimeoutError)),
    after_retry=log_retry_details,
    max_attempts=4,
    wait_seconds=1
)
def detailed_logging():
    global attempt_count_8
    attempt_count_8 += 1
    
    if attempt_count_8 == 1:
        raise ConnectionError("DNS lookup failed")
    elif attempt_count_8 == 2:
        raise TimeoutError("Request timeout")
    elif attempt_count_8 == 3:
        raise ConnectionError("Connection refused")
    return "Success!"

result = detailed_logging()
print(f"  Result: {result}\n")
attempt_count_8 = 0

# %%
# Example 9: Progress Indicator
print("Example 9: Progress Indicator")
print("-" * 50)

attempt_count_9 = 0

def show_progress(retry_state):
    attempt = retry_state.attempt_number
    max_attempts = retry_state.retry_object.stop.max_attempt_number
    filled = "█" * attempt
    empty = "░" * (max_attempts - attempt)
    percentage = (attempt / max_attempts) * 100
    print(f"  [{filled}{empty}] {percentage:.0f}% - Attempt {attempt}/{max_attempts}")

@retry_args(
    before_retry=show_progress,
    max_attempts=4,
    wait_seconds=1
)
def progress_retry():
    global attempt_count_9
    attempt_count_9 += 1
    if attempt_count_9 < 4:
        raise RuntimeError("Still processing")
    return "Complete!"

result = progress_retry()
print(f"  Result: {result}\n")
attempt_count_9 = 0

# %%
# Example 10: Timing Information
print("Example 10: Timing Information")
print("-" * 50)

attempt_count_10 = 0

def before_timing(retry_state):
    print(f"  ⏳ Starting attempt {retry_state.attempt_number}...")
    retry_state.start_time = time.time()

def after_timing(retry_state):
    if hasattr(retry_state, 'start_time'):
        elapsed = time.time() - retry_state.start_time
        print(f"  ⏱️  Attempt {retry_state.attempt_number} took {elapsed:.2f}s")

@retry_args(
    before_retry=before_timing,
    after_retry=after_timing,
    max_attempts=3,
    wait_seconds=1
)
def timed_retry():
    global attempt_count_10
    attempt_count_10 += 1
    time.sleep(0.3)  # Simulate work
    if attempt_count_10 < 3:
        raise RuntimeError("Not ready")
    return "Done!"

result = timed_retry()
print(f"  Result: {result}\n")
attempt_count_10 = 0

# %% [markdown]
"""
## Class-Based Usage

Using retry_args with classes and instance attributes.
"""

# %%

# Example 11: Class-Based Retry Configuration
print("Example 11: Class-Based Retry Configuration")
print("-" * 50)

class APIClient:
    """Example class using retry_args with instance attributes"""
    
    retry_max_attempts = 3  # Default retry configuration
    retry_wait = 1
    
    def __init__(self, name):
        self.name = name
        self.call_count = 0
    
    @retry_args(retry_conditions=retry_if_exception_type(ConnectionError))
    def get_data(self, endpoint):
        """Uses instance retry_max_attempts and retry_wait"""
        self.call_count += 1
        print(f"  [{self.name}] API call {self.call_count} to {endpoint}")
        
        if self.call_count < 3:
            raise ConnectionError("Network error")
        return {"endpoint": endpoint, "data": "success"}
    
    @retry_args(
        retry_conditions=retry_if_result(lambda x: x.get("status") != "ready"),
        max_attempts=4  # Override instance attribute
    )
    def wait_for_ready(self):
        """Override instance attributes with explicit parameters"""
        self.call_count += 1
        print(f"  [{self.name}] Status check {self.call_count}")
        
        if self.call_count < 3:
            return {"status": "pending"}
        return {"status": "ready", "data": "available"}

# Test the class
client = APIClient("MyService")
result = client.get_data("/users")
print(f"  Result: {result}")

# Reset and test override
client.call_count = 0
result = client.wait_for_ready()
print(f"  Result: {result}\n")

# %% [markdown]
"""
## Real-World Examples

Practical examples you might use in production code.
"""

# %%
# Example 12: Database Connection with Retry
print("Example 12: Database Connection with Retry")
print("-" * 50)

db_attempt = 0

@retry_args(
    retry_conditions=retry_if_exception_type((ConnectionError, TimeoutError)),
    max_attempts=5,
    wait_seconds=2,
    after_retry=lambda s: print(f"  🔌 Database connection attempt {s.attempt_number} failed, retrying...")
)
def connect_to_database():
    global db_attempt
    db_attempt += 1
    print(f"  Connecting to database... (attempt {db_attempt})")
    
    if db_attempt < 4:
        raise ConnectionError("Database unreachable")
    return "Connected to database"

result = connect_to_database()
print(f"  Result: {result}\n")
db_attempt = 0

# %%
# Example 13: API Call with Validation
print("Example 13: API Call with Validation")
print("-" * 50)

api_attempt = 0

@retry_args(
    retry_conditions=[
        retry_if_exception_type((ConnectionError, TimeoutError)),
        retry_if_result(lambda x: x is None or x.get("error"))
    ],
    max_attempts=3,
    wait_seconds=1,
    after_retry=lambda s: print(f"  🌐 API retry {s.attempt_number}: request failed")
)
def api_call_with_validation():
    global api_attempt
    api_attempt += 1
    print(f"  Making API call... (attempt {api_attempt})")
    
    if api_attempt == 1:
        raise ConnectionError("Network timeout")
    elif api_attempt == 2:
        return {"error": "Invalid request"}
    return {"success": True, "data": [1, 2, 3]}

result = api_call_with_validation()
print(f"  Result: {result}\n")
api_attempt = 0

# %% [markdown]
"""
## Quick Reference Guide

Copy these patterns for common use cases.
"""

# %%
# Quick Reference Guide
print("QUICK REFERENCE GUIDE")
print("=" * 50)
print("""
BASIC USAGE:
@retry_args(max_attempts=3, wait_seconds=1)

SPECIFIC EXCEPTIONS:
@retry_args(retry_conditions=retry_if_exception_type(ConnectionError))

MULTIPLE EXCEPTIONS:
@retry_args(retry_conditions=retry_if_exception_type((ConnectionError, TimeoutError)))

RESULT-BASED:
@retry_args(retry_conditions=retry_if_result(lambda x: x is None))

COMBINED CONDITIONS:
@retry_args(retry_conditions=[
    retry_if_exception_type(ConnectionError),
    retry_if_result(lambda x: x is None)
])

LOGGING:
- before_retry: Called before each retry (outcome is None)
- after_retry: Called after each retry (outcome available)

INSTANCE ATTRIBUTES:
class MyClass:
    retry_max_attempts = 3
    retry_wait = 1
    
    @retry_args(retry_conditions=...)
    def my_method(self):
        pass

TENACITY CONDITIONS YOU CAN USE:
- retry_if_exception_type(Exception)
- retry_if_result(lambda x: condition)
- retry_if_not_result(lambda x: condition)
- retry_if_exception_message(match="pattern")
""")
print("=" * 50)
print("Tutorial Complete! Copy any pattern above for your use case.")
print("=" * 50)