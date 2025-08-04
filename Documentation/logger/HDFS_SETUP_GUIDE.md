## HDFS Copy Methods Available

LogManager now has these new methods:

1. **`start_hdfs_copy(copy_name, local_pattern, hdfs_destination, ...)`**
   - Starts background copying from local to HDFS
   - Runs every 60 seconds by default (configurable)
   - Non-blocking operation

2. **`stop_hdfs_copy(copy_name)`**
   - Stops a specific copy operation
   - Returns True if successfully stopped

3. **`stop_all_hdfs_copy()`**
   - Stops all running copy operations
   - Returns list of operations that failed to stop

4. **`list_hdfs_copy_operations()`**
   - Lists all active copy operations
   - Shows thread status and configuration

## Example Usage

```python
from src.main.logger import LogManager

# Create LogManager instance
log_manager = LogManager()

# Start copying log files to HDFS every minute
log_manager.start_hdfs_copy(
    copy_name="main_logs",
    local_pattern="/path/to/logs/*.log",
    hdfs_destination="/hdfs/user/logs/",
    copy_interval=60  # seconds
)

# Check running operations
operations = log_manager.list_hdfs_copy_operations()
print(f"Active HDFS copy operations: {len(operations)}")

# Stop when done
log_manager.stop_hdfs_copy("main_logs")
```

## Features Included

- ✅ Background threading for non-blocking operation
- ✅ Configurable copy intervals
- ✅ Pattern-based file matching (glob patterns)
- ✅ Retry logic with exponential backoff
- ✅ Proper error handling and logging
- ✅ Thread-safe operation management
- ✅ Directory structure preservation options
- ✅ Multiple concurrent copy operations
- ✅ Comprehensive test coverage

