# LogManager Ray Cluster Guide

## Overview

When running your LogManager in a Ray cluster environment, you need to handle HDFS copy operations carefully to avoid conflicts and duplicate transfers. This guide explains the considerations and provides simple solutions.

## The Problem

In a Ray cluster, you have multiple replicas of your code running simultaneously:

- **Head Node**: Coordinates the cluster and runs scheduling
- **Worker Nodes**: Execute distributed tasks  
- **Multiple Processes**: Each task/actor may create its own LogManager instance

Without coordination, this leads to:
- ❌ Multiple LogManager instances copying the same files
- ❌ Race conditions and file conflicts
- ❌ Wasted network bandwidth and HDFS resources
- ❌ Potential data corruption from concurrent writes

## The Solution: Automatic Ray Coordination

Your LogManager now includes **automatic Ray cluster coordination** that:

✅ **Detects Ray environment** automatically  
✅ **Only runs HDFS copy on the head node**  
✅ **Disables HDFS copy on worker nodes**  
✅ **Works seamlessly in standalone mode**  
✅ **Requires zero code changes**

## How It Works

### 1. Automatic Detection

```python
# LogManager automatically detects the environment:

# Standalone mode (no Ray)
log_manager = LogManager()
log_manager.start_hdfs_copy(...)  # ✅ Runs normally

# Ray head node  
log_manager = LogManager()
log_manager.start_hdfs_copy(...)  # ✅ Runs normally

# Ray worker node
log_manager = LogManager()
log_manager.start_hdfs_copy(...)  # ⏸️ Automatically disabled
```

### 2. Environment Variable Override

```bash
# Force disable HDFS copy on any node
export RAY_DISABLE_HDFS_COPY=true

# Your application code stays the same
python your_app.py
```

### 3. Ray Task Example

```python
import ray
from utilities.logger import LogManager

@ray.remote
def distributed_task(task_id: int):
    # Each task gets its own LogManager
    log_manager = LogManager()
    
    # Get logger and log normally
    logger = log_manager.get_logger(f"task_{task_id}")
    logger.info(f"Processing task {task_id}")
    
    # HDFS copy setup (only works on head node)
    log_manager.start_hdfs_copy(
        copy_name=f"task_{task_id}_logs",
        path_patterns=[f"/tmp/task_{task_id}/*.log"],
        hdfs_destination="hdfs://cluster/task_logs/"
    )
    # ↑ This is automatically disabled on worker nodes
    
    return f"Task {task_id} completed"

# Submit tasks - each gets isolated logging
futures = [distributed_task.remote(i) for i in range(10)]
results = ray.get(futures)
```

## Best Practices

### 1. **Simple Approach (Recommended)**

Just use LogManager normally. The Ray coordination is automatic:

```python
# This works correctly in ALL environments
def setup_logging():
    log_manager = LogManager()
    
    # Always call this - it's automatically coordinated
    log_manager.start_hdfs_copy(
        copy_name="app_logs",
        path_patterns=["/app/logs/*.log"],
        hdfs_destination="hdfs://cluster/logs/"
    )
    
    return log_manager
```

### 2. **Centralized Log Collection**

Structure your logging to collect from all nodes:

```python
# On each Ray node/task
log_manager = LogManager()

# Use unique paths per node to avoid conflicts
import socket
node_id = socket.gethostname()

# Configure file handlers with node-specific paths
logger = log_manager.get_logger("app")
logger.add(f"/tmp/logs/{node_id}/app.log")

# HDFS copy on head node will collect all node logs
log_manager.start_hdfs_copy(
    copy_name="cluster_logs",
    path_patterns=["/tmp/logs/*/*.log"],  # Collects from all nodes
    hdfs_destination="hdfs://cluster/distributed_logs/",
    preserve_structure=True  # Maintains node directory structure
)
```

### 3. **Manual Control**

If you need explicit control:

```python
import os

# Method 1: Environment variable
os.environ['RAY_DISABLE_HDFS_COPY'] = 'true'  # Disable on this process

# Method 2: Check Ray status yourself
try:
    import ray
    if ray.is_initialized():
        # Your custom logic here
        current_node = ray.get_runtime_context().node_id
        # ... decide based on node type
except ImportError:
    # Not a Ray environment
    pass
```

## Configuration Options

### LogManager Initialization

```python
# Standard initialization - Ray coordination is automatic
log_manager = LogManager(
    config_path="config.yaml",
    timezone="Asia/Singapore"
)
```

### HDFS Copy Parameters

All existing parameters work the same:

```python
log_manager.start_hdfs_copy(
    copy_name="coordinated_copy",
    path_patterns=["/app/logs/*.log", "/app/logs/*.txt"],
    hdfs_destination="hdfs://namenode:9000/logs/",
    root_dir="/app/",
    copy_interval=60,  # Copy every minute
    create_dest_dirs=True,
    preserve_structure=True,  # Maintain directory structure
    max_retries=3,
    retry_delay=5
)
```

## Monitoring

### Check Ray Coordination Status

```python
# Check if HDFS copy is enabled
log_manager = LogManager()
print(f"HDFS copy enabled: {log_manager._ray_hdfs_enabled}")

# List active operations (only on head node)
operations = log_manager.list_hdfs_copy_operations()
print(f"Active HDFS operations: {len(operations)}")
```

### Logs

Look for these messages in your output:

```
Ray head node detected: HDFS copy enabled
Ray worker node detected: HDFS copy disabled  
HDFS copy operation 'app_logs' skipped: disabled in Ray cluster worker node
```

## Troubleshooting

### Common Issues

1. **"HDFS copy disabled" on head node**
   ```bash
   # Check environment variable
   echo $RAY_DISABLE_HDFS_COPY
   
   # Should be empty or 'false' on head node
   unset RAY_DISABLE_HDFS_COPY
   ```

2. **HDFS copy running on worker nodes**
   ```python
   # Force disable on workers
   os.environ['RAY_DISABLE_HDFS_COPY'] = 'true'
   ```

3. **No HDFS copy in standalone mode**
   ```python
   # Check Ray detection
   log_manager = LogManager()
   print(f"Ray coordination enabled: {log_manager._ray_hdfs_enabled}")
   # Should be True in standalone mode
   ```

### Debug Ray Detection

```python
def debug_ray_environment():
    try:
        import ray
        if ray.is_initialized():
            nodes = ray.nodes()
            current_node = ray.get_runtime_context().node_id.hex()
            
            for node in nodes:
                is_current = node['NodeID'] == current_node
                print(f"Node {node['NodeID'][:8]}: "
                      f"{'(current)' if is_current else ''} "
                      f"Resources: {node.get('Resources', {})}")
        else:
            print("Ray not initialized")
    except ImportError:
        print("Ray not installed")

debug_ray_environment()
```

## Summary

Your LogManager now "just works" in Ray clusters with **zero code changes required**:

- ✅ Automatic Ray detection
- ✅ Head-node-only HDFS copy  
- ✅ Worker node coordination
- ✅ Standalone mode compatibility
- ✅ Environment variable overrides

Simply use LogManager as you normally would - the Ray coordination is handled automatically behind the scenes.
