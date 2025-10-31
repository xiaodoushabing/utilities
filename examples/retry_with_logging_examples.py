"""
Examples demonstrating retry_args with before_retry and after_retry callbacks.

Shows how to access attempt numbers and log retry attempts elegantly.
"""

import sys
import os

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src', 'main'))

from _aux._aux import retry_args
from tenacity import retry_if_exception_type, retry_if_result


# ============================================================================
# Example 1: Simple Logging - Attempt Number
# ============================================================================

print("=" * 70)
print("Example 1: Log Attempt Number")
print("=" * 70)

attempt_count_1 = 0

@retry_args(
    max_attempts=3,
    wait_seconds=1,
    before_retry=lambda s: print(f"  🔄 Retrying... Attempt {s.attempt_number}")
)
def fetch_with_logging():
    global attempt_count_1
    attempt_count_1 += 1
    if attempt_count_1 < 3:
        raise ConnectionError("Network error")
    return "Success!"

try:
    result = fetch_with_logging()
    print(f"  ✅ Result: {result}\n")
    attempt_count_1 = 0
except Exception as e:
    print(f"  ❌ Failed: {e}\n")


# ============================================================================
# Example 2: Detailed Logging Function
# ============================================================================

print("=" * 70)
print("Example 2: Detailed Logging with Custom Function")
print("=" * 70)

def log_retry_attempt(retry_state):
    """Custom logging function with full retry state info."""
    attempt = retry_state.attempt_number
    max_attempts = retry_state.retry_object.stop.max_attempt_number
    
    print(f"  ⚠️  Attempt {attempt}/{max_attempts} failed")
    
    # Check if it was an exception or bad result
    # Note: outcome is only available in after_retry callbacks, not before_retry
    if retry_state.outcome is not None and retry_state.outcome.failed:
        exc = retry_state.outcome.exception()
        print(f"     Error: {type(exc).__name__}: {exc}")
    elif retry_state.outcome is not None:
        result = retry_state.outcome.result()
        print(f"     Bad result: {result}")
    else:
        print(f"     About to retry attempt {attempt}")

attempt_count_2 = 0

@retry_args(
    retry_conditions=retry_if_exception_type(ConnectionError),
    after_retry=log_retry_attempt,  # Changed from before_retry to after_retry
    max_attempts=4,
    wait_seconds=1
)
def fetch_with_detailed_logging():
    global attempt_count_2
    attempt_count_2 += 1
    
    if attempt_count_2 < 4:
        raise ConnectionError(f"Connection attempt {attempt_count_2} failed")
    return "Success!"

try:
    result = fetch_with_detailed_logging()
    print(f"  ✅ Final result: {result}\n")
    attempt_count_2 = 0
except Exception as e:
    print(f"  ❌ All attempts failed: {e}\n")


# ============================================================================
# Example 3: Before AND After Callbacks
# ============================================================================

print("=" * 70)
print("Example 3: Using Both before_retry and after_retry")
print("=" * 70)

import time

def before_callback(retry_state):
    print(f"  ⏳ Starting attempt {retry_state.attempt_number}...")
    # You could add timing info here
    retry_state.start_time = time.time()

def after_callback(retry_state):
    elapsed = time.time() - retry_state.start_time
    print(f"  ⏱️  Attempt {retry_state.attempt_number} took {elapsed:.2f}s")

attempt_count_3 = 0

@retry_args(
    before_retry=before_callback,
    after_retry=after_callback,
    max_attempts=3,
    wait_seconds=1
)
def slow_operation():
    global attempt_count_3
    attempt_count_3 += 1
    time.sleep(0.5)  # Simulate slow operation
    
    if attempt_count_3 < 3:
        raise RuntimeError("Not ready yet")
    return "Done!"

try:
    result = slow_operation()
    print(f"  ✅ Result: {result}\n")
    attempt_count_3 = 0
except Exception as e:
    print(f"  ❌ Failed: {e}\n")


# ============================================================================
# Example 4: Logging with Result-Based Retries
# ============================================================================

print("=" * 70)
print("Example 4: Log Retries Based on Result")
print("=" * 70)

def log_bad_result(retry_state):
    # Note: outcome is only available in after_retry callbacks
    if retry_state.outcome is not None:
        result = retry_state.outcome.result()
        attempt = retry_state.attempt_number
        print(f"  ⚠️  Attempt {attempt} returned: {result} (retrying...)")

attempt_count_4 = 0

@retry_args(
    retry_conditions=retry_if_result(lambda x: x is None or x == []),
    after_retry=log_bad_result,  # Changed from before_retry to after_retry
    max_attempts=4,
    wait_seconds=1
)
def fetch_until_valid():
    global attempt_count_4
    attempt_count_4 += 1
    
    if attempt_count_4 == 1:
        return None
    elif attempt_count_4 == 2:
        return []
    elif attempt_count_4 == 3:
        return None
    return ["data1", "data2"]

result = fetch_until_valid()
print(f"  ✅ Final result: {result}\n")
attempt_count_4 = 0


# ============================================================================
# Example 5: Progress Indicator
# ============================================================================

print("=" * 70)
print("Example 5: Progress Indicator During Retries")
print("=" * 70)

def show_progress(retry_state):
    attempt = retry_state.attempt_number
    max_attempts = retry_state.retry_object.stop.max_attempt_number
    
    # Create progress bar
    filled = "█" * attempt
    empty = "░" * (max_attempts - attempt)
    percentage = (attempt / max_attempts) * 100
    
    print(f"  [{filled}{empty}] {percentage:.0f}% - Attempt {attempt}/{max_attempts}")

attempt_count_5 = 0

@retry_args(
    before_retry=show_progress,
    max_attempts=5,
    wait_seconds=1
)
def long_retry_operation():
    global attempt_count_5
    attempt_count_5 += 1
    
    if attempt_count_5 < 5:
        raise RuntimeError("Still failing")
    return "Success!"

try:
    result = long_retry_operation()
    print(f"  ✅ Result: {result}\n")
    attempt_count_5 = 0
except Exception as e:
    print(f"  ❌ Failed: {e}\n")


# ============================================================================
# Example 6: Class-Based Logging with Instance Methods
# ============================================================================

print("=" * 70)
print("Example 6: Class with Instance Method Callbacks")
print("=" * 70)

class APIClient:
    def __init__(self, name):
        self.name = name
        self.attempt_count = 0
        self.logger_calls = []
    
    def log_retry(self, retry_state):
        """Instance method as callback - has access to self!"""
        attempt = retry_state.attempt_number
        self.logger_calls.append(attempt)
        print(f"  [{self.name}] Retry attempt {attempt}")
    
    @retry_args(
        retry_conditions=retry_if_exception_type(ConnectionError),
        max_attempts=3,
        wait_seconds=1
    )
    def fetch_with_external_logger(self, before_callback):
        """Pass callback dynamically."""
        # Note: This demonstrates the limitation - callbacks are set at decoration time
        # For dynamic callbacks, see next example
        self.attempt_count += 1
        if self.attempt_count < 3:
            raise ConnectionError("Network error")
        return {"data": "success"}

# Create client
client = APIClient("MyAPI")

# Use lambda to capture self
@retry_args(
    before_retry=lambda s: client.log_retry(s),
    max_attempts=3,
    wait_seconds=1
)
def fetch_for_client():
    client.attempt_count += 1
    if client.attempt_count < 3:
        raise ConnectionError("Network error")
    return {"data": "success"}

try:
    result = fetch_for_client()
    print(f"  ✅ Result: {result}")
    print(f"  📊 Logged {len(client.logger_calls)} retry attempts: {client.logger_calls}\n")
except Exception as e:
    print(f"  ❌ Failed: {e}\n")


# ============================================================================
# Example 7: Accessing Exception Details
# ============================================================================

print("=" * 70)
print("Example 7: Log Exception Type and Message")
print("=" * 70)

def log_exception_details(retry_state):
    # Note: outcome is only available in after_retry callbacks
    if retry_state.outcome is not None and retry_state.outcome.failed:
        exc = retry_state.outcome.exception()
        exc_type = type(exc).__name__
        exc_msg = str(exc)
        attempt = retry_state.attempt_number
        
        print(f"  ❌ Attempt {attempt}: {exc_type} - {exc_msg}")

attempt_count_7 = 0

@retry_args(
    after_retry=log_exception_details,  # Changed from before_retry to after_retry
    max_attempts=4,
    wait_seconds=1
)
def various_errors():
    global attempt_count_7
    attempt_count_7 += 1
    
    if attempt_count_7 == 1:
        raise ConnectionError("DNS lookup failed")
    elif attempt_count_7 == 2:
        raise TimeoutError("Request timed out after 30s")
    elif attempt_count_7 == 3:
        raise RuntimeError("Service temporarily unavailable")
    return "Success!"

try:
    result = various_errors()
    print(f"  ✅ Result: {result}\n")
    attempt_count_7 = 0
except Exception as e:
    print(f"  ❌ Final failure: {e}\n")


# ============================================================================
# Example 8: Custom Retry State Information
# ============================================================================

print("=" * 70)
print("Example 8: Access All Retry State Information")
print("=" * 70)

def comprehensive_logging(retry_state):
    """Show all available information from retry_state."""
    print(f"\n  📋 Retry State Details:")
    print(f"     - Attempt number: {retry_state.attempt_number}")
    print(f"     - Idle for: {retry_state.idle_for}s")
    print(f"     - Next action: {retry_state.next_action}")
    
    # Note: outcome is only available in after_retry callbacks
    if retry_state.outcome is not None and retry_state.outcome.failed:
        exc = retry_state.outcome.exception()
        print(f"     - Exception: {type(exc).__name__}")
    elif retry_state.outcome is not None:
        print(f"     - Result: {retry_state.outcome.result()}")

attempt_count_8 = 0

@retry_args(
    after_retry=comprehensive_logging,  # Changed from before_retry to after_retry
    max_attempts=3,
    wait_seconds=2
)
def detailed_retry():
    global attempt_count_8
    attempt_count_8 += 1
    
    if attempt_count_8 < 3:
        raise ValueError(f"Attempt {attempt_count_8} not ready")
    return "Success!"

try:
    result = detailed_retry()
    print(f"\n  ✅ Final result: {result}\n")
    attempt_count_8 = 0
except Exception as e:
    print(f"\n  ❌ Failed: {e}\n")


# ============================================================================
# Example 9: Conditional Logging
# ============================================================================

print("=" * 70)
print("Example 9: Only Log After Multiple Failures")
print("=" * 70)

def log_if_many_attempts(retry_state):
    """Only log if we've already tried multiple times."""
    attempt = retry_state.attempt_number
    
    if attempt >= 3:
        print(f"  ⚠️  WARNING: Already {attempt} attempts - still failing!")

attempt_count_9 = 0

@retry_args(
    before_retry=log_if_many_attempts,
    max_attempts=5,
    wait_seconds=1
)
def eventually_succeeds():
    global attempt_count_9
    attempt_count_9 += 1
    
    if attempt_count_9 < 5:
        raise RuntimeError("Not ready")
    return "Success!"

try:
    result = eventually_succeeds()
    print(f"  ✅ Result: {result}\n")
    attempt_count_9 = 0
except Exception as e:
    print(f"  ❌ Failed: {e}\n")


# ============================================================================
# Summary
# ============================================================================

print("=" * 70)
print("Key Retry State Attributes:")
print("=" * 70)
print("""
retry_state.attempt_number          - Current attempt (1, 2, 3, ...)
retry_state.outcome                 - Outcome object (success/failure)
retry_state.outcome.failed          - True if exception was raised
retry_state.outcome.exception()     - Get the exception object
retry_state.outcome.result()        - Get the result value
retry_state.retry_object            - Access to retry configuration
retry_state.idle_for                - Time waited before this attempt
retry_state.next_action             - What happens next
retry_state.args                    - Function arguments
retry_state.kwargs                  - Function keyword arguments

IMPORTANT: Callback Timing
- before_retry: Called BEFORE each retry attempt
  * retry_state.outcome is None (function hasn't run yet)
  * Use for: logging attempt number, preparing for retry
- after_retry: Called AFTER each retry attempt  
  * retry_state.outcome contains the result/exception
  * Use for: logging exceptions, results, timing

Common Patterns:
- before_retry: Log before each retry (see attempt number)
- after_retry: Log after each retry (access exceptions/results)
- Access exception: retry_state.outcome.exception() (after_retry only)
- Access result: retry_state.outcome.result() (after_retry only)
""")
print("=" * 70)
