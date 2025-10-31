# Retry Callbacks - Logging Attempt Numbers

## 🎯 The Elegant Solution

Tenacity provides **built-in callbacks** for retry lifecycle events. Simply pass callback functions to access attempt numbers and retry state!

---

## ✨ Quick Start

### Basic Logging

```python
from _aux._aux import retry_args

@retry_args(
    max_attempts=3,
    before_retry=lambda s: print(f"Retry attempt {s.attempt_number}")
)
def my_function():
    pass
```

### Custom Logging Function

```python
def log_retry(retry_state):
    attempt = retry_state.attempt_number
    max_attempts = retry_state.retry_object.stop.max_attempt_number
    print(f"Attempt {attempt}/{max_attempts}")

@retry_args(before_retry=log_retry, max_attempts=5)
def my_function():
    pass
```

---

## 📋 Retry State Object

The `retry_state` object passed to callbacks contains:

| Attribute | Description | Example |
|-----------|-------------|---------|
| `attempt_number` | Current attempt (1, 2, 3, ...) | `s.attempt_number` |
| `outcome` | Result or exception info | `s.outcome` |
| `outcome.failed` | True if exception raised | `if s.outcome.failed:` |
| `outcome.exception()` | Get exception object | `exc = s.outcome.exception()` |
| `outcome.result()` | Get return value | `result = s.outcome.result()` |
| `retry_object` | Access retry configuration | `s.retry_object.stop` |
| `idle_for` | Time waited before this attempt | `s.idle_for` |
| `args` | Function positional arguments | `s.args` |
| `kwargs` | Function keyword arguments | `s.kwargs` |

---

## 🔧 Two Callback Types

### 1. `before_retry` - Called BEFORE Each Retry

```python
def before_callback(retry_state):
    print(f"About to retry attempt {retry_state.attempt_number}")

@retry_args(before_retry=before_callback)
def fetch():
    pass
```

**Use cases:**
- Log attempt number
- Track retry metrics
- Modify behavior based on attempt count

### 2. `after_retry` - Called AFTER Each Retry

```python
def after_callback(retry_state):
    print(f"Completed attempt {retry_state.attempt_number}")

@retry_args(after_retry=after_callback)
def fetch():
    pass
```

**Use cases:**
- Measure attempt duration
- Log results/errors
- Cleanup after failed attempts

---

## 💡 Common Patterns

### Pattern 1: Simple Attempt Counter

```python
@retry_args(
    before_retry=lambda s: print(f"🔄 Retry {s.attempt_number}"),
    max_attempts=3
)
def fetch():
    pass
```

### Pattern 2: Detailed Error Logging

```python
def log_error(retry_state):
    if retry_state.outcome.failed:
        exc = retry_state.outcome.exception()
        print(f"Attempt {retry_state.attempt_number} failed: {exc}")

@retry_args(before_retry=log_error)
def fetch():
    pass
```

### Pattern 3: Progress Bar

```python
def show_progress(retry_state):
    attempt = retry_state.attempt_number
    max_attempts = retry_state.retry_object.stop.max_attempt_number
    percentage = (attempt / max_attempts) * 100
    print(f"[{'█' * attempt}{'░' * (max_attempts - attempt)}] {percentage:.0f}%")

@retry_args(before_retry=show_progress, max_attempts=5)
def fetch():
    pass
```

### Pattern 4: Both Before and After

```python
import time

def before(retry_state):
    retry_state.start_time = time.time()
    print(f"Starting attempt {retry_state.attempt_number}")

def after(retry_state):
    duration = time.time() - retry_state.start_time
    print(f"Attempt took {duration:.2f}s")

@retry_args(before_retry=before, after_retry=after)
def fetch():
    pass
```

### Pattern 5: Instance Method as Callback

```python
class APIClient:
    def log_retry(self, retry_state):
        print(f"[{self.name}] Retry {retry_state.attempt_number}")
    
    def fetch(self):
        # Use lambda to capture self
        @retry_args(before_retry=lambda s: self.log_retry(s))
        def _fetch():
            pass
        return _fetch()
```

---

## 🎨 Real-World Examples

### Example 1: API Call with Logging

```python
from tenacity import retry_if_exception_type

def log_api_retry(retry_state):
    attempt = retry_state.attempt_number
    if retry_state.outcome.failed:
        exc = retry_state.outcome.exception()
        print(f"⚠️  API call attempt {attempt} failed: {exc}")

@retry_args(
    retry_conditions=retry_if_exception_type((ConnectionError, TimeoutError)),
    before_retry=log_api_retry,
    max_attempts=3,
    wait_seconds=2
)
def call_api():
    return requests.get("https://api.example.com/data")
```

### Example 2: Database with Metrics

```python
class DatabaseClient:
    def __init__(self):
        self.retry_count = 0
    
    def track_retry(self, retry_state):
        self.retry_count += 1
        attempt = retry_state.attempt_number
        print(f"DB retry {attempt} (total retries: {self.retry_count})")
    
    @retry_args(max_attempts=5, wait_seconds=3)
    def query_with_logging(self, sql):
        @retry_args(before_retry=lambda s: self.track_retry(s))
        def _query():
            return db.execute(sql)
        return _query()
```

### Example 3: Conditional Logging

```python
def log_if_failing_badly(retry_state):
    """Only log after 3 failed attempts."""
    if retry_state.attempt_number >= 3:
        print(f"⚠️  WARNING: {retry_state.attempt_number} attempts and still failing!")

@retry_args(
    before_retry=log_if_failing_badly,
    max_attempts=5
)
def unstable_operation():
    pass
```

---

## 🔍 Debugging Tips

### View All Retry State Info

```python
def debug_retry(retry_state):
    print(f"\n=== Retry State Debug ===")
    print(f"Attempt: {retry_state.attempt_number}")
    print(f"Idle for: {retry_state.idle_for}s")
    print(f"Next action: {retry_state.next_action}")
    
    if retry_state.outcome.failed:
        exc = retry_state.outcome.exception()
        print(f"Exception: {type(exc).__name__}: {exc}")
    else:
        print(f"Result: {retry_state.outcome.result()}")
    print("========================\n")

@retry_args(before_retry=debug_retry)
def fetch():
    pass
```

---

## ⚡ Performance Notes

- Callbacks are **very lightweight** - minimal overhead
- Only called when retries actually happen
- Can store state on `retry_state` object (see Pattern 4)

---

## 🎯 Best Practices

### ✅ DO

```python
# Simple, focused callbacks
@retry_args(before_retry=lambda s: print(f"Retry {s.attempt_number}"))

# Access exception details when failed
def log(s):
    if s.outcome.failed:
        print(s.outcome.exception())

# Track metrics
def track(s):
    metrics.increment('retries', s.attempt_number)
```

### ❌ DON'T

```python
# Don't do heavy work in callbacks
@retry_args(before_retry=lambda s: expensive_db_query())  # BAD

# Don't swallow exceptions
def bad_callback(s):
    try:
        # ... code ...
    except:
        pass  # BAD - hides errors

# Don't modify function behavior
def bad_callback(s):
    if s.attempt_number > 2:
        raise StopRetrying()  # Use retry_conditions instead
```

---

## 📚 Integration with Logging Libraries

### With Python logging

```python
import logging

logger = logging.getLogger(__name__)

def log_retry(retry_state):
    logger.warning(
        f"Retry attempt {retry_state.attempt_number}",
        extra={"attempt": retry_state.attempt_number}
    )

@retry_args(before_retry=log_retry)
def fetch():
    pass
```

### With structlog

```python
import structlog

log = structlog.get_logger()

def log_retry(retry_state):
    log.warning(
        "retry_attempt",
        attempt=retry_state.attempt_number,
        max_attempts=retry_state.retry_object.stop.max_attempt_number
    )

@retry_args(before_retry=log_retry)
def fetch():
    pass
```

---

## 🚀 Advanced: Custom Retry State Attributes

You can add custom attributes to `retry_state`:

```python
import time

def before(retry_state):
    retry_state.custom_start = time.time()
    retry_state.custom_id = f"attempt_{retry_state.attempt_number}"
    print(f"Starting {retry_state.custom_id}")

def after(retry_state):
    duration = time.time() - retry_state.custom_start
    print(f"{retry_state.custom_id} took {duration:.2f}s")

@retry_args(before_retry=before, after_retry=after)
def fetch():
    pass
```

---

## 📊 Comparison

### Before (Manual Tracking)

```python
attempt = 0
@retry_args(max_attempts=3)
def fetch():
    global attempt
    attempt += 1
    print(f"Attempt {attempt}")  # ❌ Messy, global state
```

### After (Callbacks)

```python
@retry_args(
    max_attempts=3,
    before_retry=lambda s: print(f"Attempt {s.attempt_number}")
)
def fetch():
    pass  # ✅ Clean, no global state
```

---

## 🎓 Summary

**The elegant solution for logging retry attempts:**

1. Use `before_retry` callback to access attempt numbers
2. Access `retry_state.attempt_number` for current attempt
3. Check `retry_state.outcome.failed` for exception info
4. Use lambdas for simple logging, functions for complex logic
5. Both `before_retry` and `after_retry` available

**No manual tracking needed - tenacity provides everything! ✨**
