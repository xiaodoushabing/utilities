# HDFS Log Copy Functionality

## Overview

The LogManager now includes built-in support for periodically copying log files from local storage to HDFS (Hadoop Distributed File System). This functionality addresses the need to backup logs to distributed storage while maintaining local logging performance.

## Key Features

- **Periodic Background Copying**: Automatic copying at user-defined intervals
- **Flexible File Selection**: Support for glob patterns and explicit file paths
- **Multiple Copy Operations**: Run multiple independent copy operations simultaneously
- **Retry Logic**: Built-in retry mechanism for handling network failures
- **Directory Structure Control**: Option to preserve or flatten directory structure
- **Thread Safety**: Non-blocking operations that don't interfere with logging performance
- **Graceful Cleanup**: Automatic cleanup on application exit

## API Reference

### Starting HDFS Copy Operations

```python
log_manager.start_hdfs_copy(
    copy_name="app_logs",
    local_pattern="/path/to/logs/*.log",
    hdfs_destination="hdfs://namenode:9000/logs/app/",
    copy_interval=60,
    filesystem="hdfs",
    create_dest_dirs=True,
    preserve_structure=False,
    max_retries=3,
    retry_delay=5
)
```

#### Parameters

- **copy_name** (str): Unique identifier for the copy operation
- **local_pattern** (str | List[str]): File patterns or paths to copy
  - Single pattern: `"/logs/*.log"`
  - Multiple patterns: `["/logs/*.log", "/logs/*.txt"]`
  - Explicit file: `"/logs/app.log"`
- **hdfs_destination** (str): HDFS destination directory
- **copy_interval** (int): Seconds between copy operations (default: 60)
- **filesystem** (str): Filesystem type (default: "hdfs")
- **create_dest_dirs** (bool): Create destination directories (default: True)
- **preserve_structure** (bool): Preserve local directory structure (default: True)
- **max_retries** (int): Maximum retry attempts (default: 3)
- **retry_delay** (int): Delay between retries in seconds (default: 5)

### Managing Copy Operations

```python
# Stop a specific operation
success = log_manager.stop_hdfs_copy("app_logs", timeout=10.0)

# Stop all operations
failed_ops = log_manager.stop_all_hdfs_copy(timeout=10.0)

# List active operations
operations = log_manager.list_hdfs_copy_operations()
```

## Usage Examples

### Basic Usage

```python
from src.main.logger import LogManager

# Initialize LogManager
log_manager = LogManager("config.yaml")

# Start copying all log files every 2 minutes
log_manager.start_hdfs_copy(
    copy_name="main_backup",
    local_pattern="/tmp/logs/*.log",
    hdfs_destination="hdfs://namenode:9000/backup/logs/",
    copy_interval=120
)

# Get logger and use normally
logger = log_manager.get_logger("app")
logger.info("This will be logged locally and copied to HDFS")
```

### Advanced Usage with Multiple Operations

```python
# Copy application logs frequently
log_manager.start_hdfs_copy(
    copy_name="app_logs",
    local_pattern="/var/log/myapp/*.log",
    hdfs_destination="hdfs://cluster/logs/app/",
    copy_interval=30,  # Every 30 seconds
    preserve_structure=False
)

# Copy error logs less frequently but with higher priority
log_manager.start_hdfs_copy(
    copy_name="error_logs",
    local_pattern=[
        "/var/log/myapp/*error*.log",
        "/var/log/myapp/*critical*.log"
    ],
    hdfs_destination="hdfs://cluster/logs/critical/",
    copy_interval=300,  # Every 5 minutes
    max_retries=5,
    preserve_structure=True
)

# Copy specific configuration files
log_manager.start_hdfs_copy(
    copy_name="config_backup",
    local_pattern="/etc/myapp/config.json",
    hdfs_destination="hdfs://cluster/config/",
    copy_interval=3600  # Every hour
)
```

### Directory Structure Handling

#### With `preserve_structure=True` (default):
```
Local: /var/log/myapp/module1/app.log
HDFS:  hdfs://cluster/logs/module1/app.log
```

#### With `preserve_structure=False`:
```
Local: /var/log/myapp/module1/app.log
HDFS:  hdfs://cluster/logs/app.log
```

## Configuration Integration

You can integrate HDFS copying into your logging configuration workflow:

```python
# config.yaml
formats:
  detailed: "{time} | {level} | {message}"

handlers:
  local_file:
    sink: "/tmp/logs/app.log"
    format: "detailed"
    level: "DEBUG"
    rotation: "10 MB"
    retention: "1 week"

loggers:
  app_logger:
    - handler: "local_file"
      level: "INFO"
```

```python
# main.py
log_manager = LogManager("config.yaml")

# Start HDFS backup
log_manager.start_hdfs_copy(
    copy_name="app_backup",
    local_pattern="/tmp/logs/*.log",
    hdfs_destination="hdfs://namenode:9000/app_logs/",
    copy_interval=60
)

# Use logger normally
app_logger = log_manager.get_logger("app_logger")
app_logger.info("Application started")
```

## Error Handling and Monitoring

### Retry Logic
The system automatically retries failed copy operations:
- Configurable maximum retry attempts
- Exponential backoff delay between retries
- Detailed error logging for troubleshooting

### Monitoring Copy Operations
```python
# Check active operations
operations = log_manager.list_hdfs_copy_operations()
for op in operations:
    print(f"Operation: {op['name']}")
    print(f"Status: {'Running' if op['is_alive'] else 'Stopped'}")
    print(f"Thread: {op['thread_name']}")
```

## Best Practices

### 1. File Locking and Log Rotation
```python
# Configure log rotation to avoid file locking issues
handlers:
  app_file:
    sink: "/logs/app.log"
    rotation: "100 MB"  # Rotate before files get too large
    retention: "7 days"
```

### 2. Optimal Copy Intervals
- **High-frequency logs**: 30-60 seconds
- **Normal application logs**: 2-5 minutes
- **Error logs**: 1-2 minutes (higher priority)
- **Configuration files**: 1 hour or more

### 3. Network Resilience
```python
log_manager.start_hdfs_copy(
    copy_name="resilient_copy",
    local_pattern="/logs/*.log",
    hdfs_destination="hdfs://cluster/logs/",
    copy_interval=60,
    max_retries=5,      # Increase retries for unreliable networks
    retry_delay=10      # Longer delay for network recovery
)
```

### 4. Resource Management
```python
# Stop operations when shutting down
def shutdown_handler():
    failed = log_manager.stop_all_hdfs_copy(timeout=15.0)
    if failed:
        print(f"Warning: Could not stop operations: {failed}")

import atexit
atexit.register(shutdown_handler)
```

## Potential Issues and Solutions

### 1. Network Connectivity
**Issue**: HDFS copy operations fail due to network issues
**Solution**: 
- Use appropriate retry settings
- Monitor network connectivity
- Consider backup local storage

### 2. File Locking
**Issue**: Log files being written while copying
**Solution**: 
- Use log rotation to create complete files
- Copy rotated files rather than active log files
- Consider file locking mechanisms

### 3. Storage Space
**Issue**: HDFS runs out of space
**Solution**: 
- Monitor HDFS disk usage
- Implement log retention policies
- Use compression for older logs

### 4. Performance Impact
**Issue**: Frequent copying affects system performance
**Solution**: 
- Adjust copy intervals based on system load
- Copy during low-usage periods
- Monitor system resources

### 5. Permission Issues
**Issue**: Access denied errors
**Solution**: 
- Ensure read permissions on local files
- Verify HDFS write permissions
- Check user authentication

## Thread Safety

The HDFS copy functionality is designed to be thread-safe:
- Each copy operation runs in its own daemon thread
- No interference with Python logging operations
- Independent retry logic per operation
- Safe concurrent access to different files

## Integration with FileIO

The implementation leverages your existing FileIO interface:
- Uses `FileIOInterface.fcopy()` for file copying
- Supports all filesystems supported by your FileIO layer
- Consistent error handling and retry logic
- Unified API across different storage systems

## Performance Considerations

- **Local logging performance**: Unaffected by HDFS operations
- **Memory usage**: Minimal overhead per copy operation
- **CPU usage**: Background threads use minimal CPU
- **Network bandwidth**: Configurable through copy intervals
- **Disk I/O**: Optimized through batched operations

This implementation provides a robust, production-ready solution for backing up logs to HDFS while maintaining the performance and reliability of your local logging system.
