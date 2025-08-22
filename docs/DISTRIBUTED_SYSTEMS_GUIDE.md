# LogManager Distributed Systems Guide

## Overview

When running your LogManager in distributed computing environments (Ray, Dask, Spark, Kubernetes, etc.), you need to coordinate HDFS copy operations to avoid conflicts and duplicate transfers. This guide explains how to control HDFS copy behavior across different distributed systems.

## The Problem

In distributed environments, multiple instances of your application run simultaneously:

- **Coordinators/Masters**: Manage the cluster and coordinate work
- **Workers/Executors**: Execute distributed tasks
- **Multiple Processes**: Each task may create its own LogManager instance

Without coordination, this leads to:
- ❌ Multiple LogManager instances copying the same files
- ❌ Race conditions and file conflicts  
- ❌ Wasted network bandwidth and storage resources
- ❌ Potential data corruption from concurrent writes

## The Solution: Environment-Based Coordination

Your LogManager now includes **flexible distributed system coordination** that:

✅ **Works with any distributed system** (Ray, Dask, Spark, Kubernetes, Slurm, etc.)  
✅ **Auto-detects common distributed environments**  
✅ **Provides manual control via environment variables**  
✅ **Requires zero code changes**  
✅ **Maintains standalone compatibility**

## Environment Variable Controls

### Method 1: Direct Control (Simplest)

```bash
# Completely disable HDFS copy on any node
export DISABLE_HDFS_COPY=true

# Your application code stays unchanged
python your_app.py
```

### Method 2: Role-Based Control

```bash
# On coordinator/master nodes
export HDFS_COPY_NODE_ROLE=coordinator

# On worker nodes  
export HDFS_COPY_NODE_ROLE=worker

# Your LogManager automatically respects these settings
```

### Method 3: Explicit Mode Control

```bash
# Force enable
export HDFS_COPY_MODE=enabled

# Force disable
export HDFS_COPY_MODE=disabled
```

## Supported Distributed Systems

### 1. **Ray Clusters**

**Automatic Detection:** LogManager detects Ray head vs worker nodes

```python
# Works automatically - no code changes needed
import ray

@ray.remote
def distributed_task():
    log_manager = LogManager()  # Auto-detects Ray environment
    log_manager.start_hdfs_copy(...)  # Only runs on head node
    return "done"
```

**Manual Control:**
```bash
# On Ray head node (enable HDFS copy)
export HDFS_COPY_NODE_ROLE=coordinator

# On Ray worker nodes (disable HDFS copy)  
export HDFS_COPY_NODE_ROLE=worker
```

### 2. **Dask Clusters**

**Automatic Detection:** LogManager detects Dask scheduler vs worker nodes

```python
from dask.distributed import Client
client = Client('scheduler-address:8786')

# LogManager automatically detects if running on scheduler or worker
log_manager = LogManager()
log_manager.start_hdfs_copy(...)  # Only runs on scheduler
```

**Manual Control:**
```bash
# On Dask scheduler
export HDFS_COPY_NODE_ROLE=coordinator

# On Dask workers
export HDFS_COPY_NODE_ROLE=worker
```

### 3. **Apache Spark**

**Automatic Detection:** LogManager detects Spark driver vs executor

```python
from pyspark import SparkContext
sc = SparkContext()

# LogManager automatically detects driver vs executor
log_manager = LogManager()
log_manager.start_hdfs_copy(...)  # Only runs on driver
```

**Manual Control:**
```bash
# In Spark driver
export HDFS_COPY_NODE_ROLE=coordinator

# In Spark executors  
export HDFS_COPY_NODE_ROLE=worker
```

### 4. **Kubernetes**

**Pod Role Detection:** Uses pod environment variables and hostname patterns

```yaml
# In your Kubernetes deployment
apiVersion: apps/v1
kind: Deployment
metadata:
  name: my-app
spec:
  template:
    spec:
      containers:
      - name: coordinator
        env:
        - name: POD_ROLE
          value: "coordinator"
        # OR
        - name: HDFS_COPY_NODE_ROLE  
          value: "coordinator"
---
apiVersion: apps/v1
kind: Deployment  
metadata:
  name: my-workers
spec:
  template:
    spec:
      containers:
      - name: worker
        env:
        - name: POD_ROLE
          value: "worker"
        # OR
        - name: HDFS_COPY_NODE_ROLE
          value: "worker"
```

**Hostname-Based Detection:**
```bash
# Coordinator pods (enable HDFS copy)
kubectl run coordinator-pod --image=myapp

# Worker pods (disable HDFS copy)  
kubectl run worker-pod --image=myapp
```

### 5. **Slurm Clusters**

**Automatic Detection:** Uses `SLURM_PROCID` to detect main task (rank 0)

```bash
#!/bin/bash
#SBATCH --job-name=distributed_app
#SBATCH --nodes=4
#SBATCH --tasks-per-node=1

# LogManager automatically detects rank 0 as coordinator
srun python your_app.py
```

**Manual Control:**
```bash
# For main task (enable HDFS copy)
export HDFS_COPY_NODE_ROLE=coordinator

# For worker tasks (disable HDFS copy)
export HDFS_COPY_NODE_ROLE=worker
```

### 6. **Generic Clusters**

For any distributed system not explicitly supported:

```bash
# Set these environment variables to control behavior

# On coordinator/master node
export WORKER_ID=0
export NODE_RANK=0  
export LOCAL_RANK=0
export HDFS_COPY_NODE_ROLE=coordinator

# On worker nodes
export WORKER_ID=1  # or 2, 3, etc.
export NODE_RANK=1  # or 2, 3, etc.
export HDFS_COPY_NODE_ROLE=worker
```

## Usage Examples

### Simple Approach (Recommended)

```python
# This code works in ALL environments with zero changes:

def setup_logging():
    log_manager = LogManager()
    
    # Always call this - coordination is automatic
    log_manager.start_hdfs_copy(
        copy_name="app_logs",
        path_patterns=["/app/logs/*.log"],
        hdfs_destination="hdfs://cluster/logs/"
    )
    
    return log_manager

# Works correctly in:
# - Standalone mode: HDFS copy runs normally
# - Distributed coordinator: HDFS copy runs normally
# - Distributed workers: HDFS copy automatically disabled
```

### Docker Compose Example

```yaml
version: '3.8'
services:
  coordinator:
    image: myapp
    environment:
      - HDFS_COPY_NODE_ROLE=coordinator
    command: python app.py
    
  worker:
    image: myapp
    environment:
      - HDFS_COPY_NODE_ROLE=worker
    deploy:
      replicas: 3
    command: python app.py
```

### Distributed Log Collection

```python
import socket
import os

def setup_distributed_logging():
    log_manager = LogManager()
    
    # Create node-specific log paths to avoid conflicts
    node_id = socket.gethostname()
    worker_id = os.getenv('WORKER_ID', '0')
    
    # Configure logging with unique paths per node
    logger = log_manager.get_logger("app")
    logger.add(f"/tmp/logs/{node_id}/worker_{worker_id}.log")
    
    # HDFS copy (only runs on coordinator)
    log_manager.start_hdfs_copy(
        copy_name="distributed_logs",
        path_patterns=["/tmp/logs/*/*.log"],  # Collect from all nodes
        hdfs_destination="hdfs://cluster/distributed_logs/",
        preserve_structure=True,  # Maintain node directory structure
        copy_interval=60
    )
    
    return log_manager, logger

# Usage in distributed task
def distributed_task():
    log_manager, logger = setup_distributed_logging()
    
    logger.info("Task started")
    # ... your work here ...
    logger.info("Task completed")
    
    # HDFS copy coordination is automatic
```

## Environment Variable Priority

The coordination logic checks environment variables in this order:

1. **`DISABLE_HDFS_COPY=true`** → Always disables HDFS copy
2. **`HDFS_COPY_NODE_ROLE=worker`** → Disables on workers
3. **`HDFS_COPY_NODE_ROLE=coordinator`** → Enables on coordinators  
4. **`HDFS_COPY_MODE=disabled`** → Force disable
5. **`HDFS_COPY_MODE=enabled`** → Force enable
6. **Auto-detection** → Detect distributed system and role
7. **Default** → Enable (standalone mode)

## Monitoring and Debugging

### Check Coordination Status

```python
log_manager = LogManager()
print(f"HDFS copy enabled: {log_manager._hdfs_copy_enabled}")

# Check which distributed system was detected
if hasattr(log_manager, '_detected_system'):
    print(f"Detected system: {log_manager._detected_system}")
```

### Debug Output

Look for these messages in your logs:

```
# Successful coordination
HDFS copy enabled: node role set to 'coordinator'
Ray head node detected: HDFS copy enabled
Kubernetes coordinator pod detected: HDFS copy enabled

# Disabled coordination  
HDFS copy disabled: node role set to 'worker'
Dask worker detected: HDFS copy disabled
HDFS copy operation 'app_logs' skipped: disabled in distributed system environment
```

### Test Coordination

```python
import os

def test_coordination():
    # Test worker behavior
    os.environ['HDFS_COPY_NODE_ROLE'] = 'worker'
    log_manager = LogManager()
    assert log_manager._hdfs_copy_enabled == False
    
    # Test coordinator behavior
    os.environ['HDFS_COPY_NODE_ROLE'] = 'coordinator'  
    log_manager = LogManager()
    assert log_manager._hdfs_copy_enabled == True
    
    print("Coordination working correctly!")

test_coordination()
```

## Best Practices

### 1. Use Environment Variables

```bash
# In your deployment scripts/containers
export HDFS_COPY_NODE_ROLE=coordinator  # for main nodes
export HDFS_COPY_NODE_ROLE=worker       # for worker nodes
```

### 2. Centralized Configuration

```python
# config.py
import os

def get_log_manager():
    """Factory function for consistent LogManager setup."""
    return LogManager(
        config_path=os.getenv('LOG_CONFIG_PATH', 'default_config.yaml'),
        timezone=os.getenv('LOG_TIMEZONE', 'UTC')
    )

def setup_hdfs_copy(log_manager):
    """Standard HDFS copy setup."""
    log_manager.start_hdfs_copy(
        copy_name="app_logs",
        path_patterns=["/app/logs/*.log"],
        hdfs_destination=os.getenv('HDFS_LOG_DEST', 'hdfs://cluster/logs/'),
        copy_interval=int(os.getenv('HDFS_COPY_INTERVAL', '60'))
    )
```

### 3. Health Checks

```python
def health_check():
    """Check if logging and HDFS copy are working correctly."""
    log_manager = LogManager()
    
    # Check HDFS copy status
    operations = log_manager.list_hdfs_copy_operations()
    
    if log_manager._hdfs_copy_enabled:
        if len(operations) == 0:
            return {"status": "warning", "message": "HDFS copy enabled but no operations running"}
        else:
            return {"status": "ok", "message": f"{len(operations)} HDFS operations running"}
    else:
        return {"status": "ok", "message": "HDFS copy disabled (worker node)"}
```

## Troubleshooting

### HDFS Copy Not Working

```bash
# Check environment variables
env | grep -E "(HDFS|DISABLE|ROLE|MODE)"

# Expected on coordinator:
# HDFS_COPY_NODE_ROLE=coordinator
# (or no variables set)

# Expected on worker:  
# HDFS_COPY_NODE_ROLE=worker
```

### Multiple HDFS Copy Operations

```bash
# If you see duplicate copies, ensure workers are properly disabled
export HDFS_COPY_NODE_ROLE=worker

# Or force disable
export DISABLE_HDFS_COPY=true
```

### Auto-Detection Issues

```python
# Debug auto-detection
log_manager = LogManager()
print(f"Detected system: {log_manager._detect_distributed_system()}")

# Override auto-detection with explicit control
os.environ['HDFS_COPY_MODE'] = 'enabled'  # or 'disabled'
```

## Summary

Your LogManager now provides **flexible coordination for any distributed system**:

- ✅ **Universal**: Works with Ray, Dask, Spark, Kubernetes, Slurm, and custom systems
- ✅ **Simple**: Just set environment variables, no code changes needed
- ✅ **Automatic**: Auto-detects common distributed systems  
- ✅ **Flexible**: Multiple control methods for different use cases
- ✅ **Safe**: Prevents conflicts and duplicate transfers by default

The coordination is **environment-driven** and **system-agnostic**, making it easy to integrate with any distributed computing platform.
