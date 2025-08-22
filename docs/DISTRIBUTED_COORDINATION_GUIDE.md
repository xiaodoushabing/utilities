# LogManager Distributed Systems Guide

## Table of Contents
1. [Understanding the Problem](#understanding-the-problem)
2. [The Simple Solution](#the-simple-solution)
3. [How It Works](#how-it-works)
4. [Quick Start Examples](#quick-start-examples)
5. [Supported Distributed Systems](#supported-distributed-systems)
6. [Configuration Methods](#configuration-methods)
7. [Best Practices](#best-practices)
8. [Monitoring and Debugging](#monitoring-and-debugging)
9. [Troubleshooting](#troubleshooting)

---

## Understanding the Problem

When running LogManager in distributed computing environments, multiple instances of your application run simultaneously across different nodes:

- **Coordinators/Head nodes**: Manage the cluster and coordinate work
- **Workers/Executors**: Execute distributed tasks in parallel
- **Multiple Processes**: Each task may create its own LogManager instance

### The Challenge

Without coordination, this creates serious issues:

```
❌ Multiple LogManager instances copying the same files
❌ Race conditions and file conflicts  
❌ Wasted network bandwidth and storage resources
❌ Potential data corruption from concurrent writes
❌ Duplicate transfers consuming HDFS storage
```

### Real-World Scenario

```python
# Without coordination - PROBLEMATIC
import ray

@ray.remote
def worker_task():
    log_manager = LogManager()
    log_manager.start_hdfs_copy(...)  # ❌ ALL nodes try to copy!
    return process_data()

# This creates 10 concurrent HDFS copy operations! 
futures = [worker_task.remote() for _ in range(10)]
```

---

## The Simple Solution

LogManager uses **environment variables** to coordinate HDFS copy operations across distributed systems:

### Core Principle
- **Coordinator/Head nodes**: HDFS copy enabled (default behavior)
- **Worker nodes**: Set `DISABLE_HDFS_COPY=true` to disable

### Key Benefits
✅ **Simple**: Just one environment variable  
✅ **Universal**: Works with any distributed system  
✅ **Zero code changes**: Your LogManager code stays identical  
✅ **Safe**: Prevents conflicts automatically  
✅ **Flexible**: Easy to override per node/container/pod  

---

## How It Works

### Decision Flow

```
1. Check DISABLE_HDFS_COPY environment variable
   ├─ "true" → Disable HDFS copy (worker behavior)
   └─ not set or "false" → Enable HDFS copy (coordinator behavior)

2. LogManager initialization
   ├─ Coordinator: start_hdfs_copy() → Actually starts copying
   └─ Worker: start_hdfs_copy() → Prints "skipped" message, does nothing
```

### Code Behavior

```python
# This code works EVERYWHERE with no changes:
log_manager = LogManager()

# Coordinator: Starts HDFS copy operation
# Worker: Logs "HDFS copy operation skipped" and continues
log_manager.start_hdfs_copy(
    copy_name="app_logs",
    path_patterns=["/app/logs/*.log"],
    hdfs_destination="hdfs://cluster/logs/"
)

# Your application code continues normally on all nodes
logger = log_manager.get_logger("app")
logger.info("Application started")  # Works on all nodes
```

---

## Quick Start Examples

### Ray Cluster

```python
# On Ray head node: (no environment variable needed)
# On Ray worker nodes: export DISABLE_HDFS_COPY=true

import ray

@ray.remote
def worker_task():
    # This code runs on both head and worker nodes
    log_manager = LogManager()
    log_manager.start_hdfs_copy(
        copy_name="app_logs",
        path_patterns=["/app/logs/*.log"],
        hdfs_destination="hdfs://cluster/logs/"
    )
    # ↑ Only actually runs on head node due to environment variable
    
    return "done"

# Submit tasks - only head node performs HDFS copy
futures = [worker_task.remote() for _ in range(10)]
results = ray.get(futures)
```

### Spark Cluster

```python
# On Spark driver: (no environment variable needed)
# On Spark executors: export DISABLE_HDFS_COPY=true

from pyspark import SparkContext

def spark_task(data):
    log_manager = LogManager()
    log_manager.start_hdfs_copy(
        copy_name="spark_logs",
        path_patterns=["/spark/logs/*.log"],
        hdfs_destination="hdfs://cluster/spark_logs/"
    )
    # ↑ Only runs on driver, not executors
    
    return process_data(data)

sc = SparkContext()
rdd = sc.parallelize(data)
results = rdd.map(spark_task).collect()
```

### Kubernetes

```yaml
# Coordinator pod
apiVersion: v1
kind: Pod
metadata:
  name: coordinator
spec:
  containers:
  - name: app
    image: myapp
    # No DISABLE_HDFS_COPY env var = HDFS copy enabled
    
---
# Worker pods
apiVersion: apps/v1
kind: Deployment
metadata:
  name: workers
spec:
  replicas: 5
  template:
    spec:
      containers:
      - name: app
        image: myapp
        env:
        - name: DISABLE_HDFS_COPY
          value: "true"  # HDFS copy disabled
```

### Docker Compose

```yaml
version: '3.8'
services:
  coordinator:
    image: myapp
    # No environment variable = HDFS copy enabled
    command: python app.py
    
  workers:
    image: myapp
    environment:
      - DISABLE_HDFS_COPY=true  # HDFS copy disabled
    deploy:
      replicas: 3
    command: python app.py
```

---

## Supported Distributed Systems

### 1. Ray Clusters
- **Environment**: Ray head vs worker nodes
- **Usage**: Set `DISABLE_HDFS_COPY=true` on Ray workers
- **Detection**: Works with Ray's node role system

### 2. Apache Spark
- **Environment**: Spark driver vs executors
- **Usage**: Set `DISABLE_HDFS_COPY=true` on executors
- **Detection**: Compatible with Spark's distributed execution

### 3. Kubernetes
- **Environment**: Different pod roles
- **Usage**: Set environment variables in pod specs
- **Detection**: Works with any K8s deployment pattern

### 4. Dask Clusters
- **Environment**: Scheduler vs workers
- **Usage**: Set `DISABLE_HDFS_COPY=true` on Dask workers
- **Detection**: Compatible with Dask distributed

### 5. Slurm Clusters
- **Environment**: Main task vs worker tasks
- **Usage**: Set `DISABLE_HDFS_COPY=true` on non-rank-0 tasks
- **Detection**: Works with SLURM job arrays

### 6. Custom/Generic Systems
- **Environment**: Any distributed setup
- **Usage**: Manual environment variable control
- **Detection**: Works with any system where you can set env vars

---

## Configuration Methods

### Simple Environment Variable Control

```bash
# On worker nodes - disable HDFS copy
export DISABLE_HDFS_COPY=true

# On coordinator nodes - do nothing (enabled by default)
# No environment variable needed
```

### Environment Variable Logic

| Environment Variable | Value | Effect |
|---------------------|-------|--------|
| `DISABLE_HDFS_COPY` | `true` | HDFS copy disabled |
| `DISABLE_HDFS_COPY` | `false` or not set | HDFS copy enabled (default) |
| (none) | - | HDFS copy enabled (default) |

---

## Best Practices

### 1. Centralized Configuration

```python
# config.py - Use factory functions for consistency
import os

def create_log_manager():
    """Factory function for consistent LogManager setup."""
    return LogManager(
        config_path=os.getenv('LOG_CONFIG_PATH', 'default_config.yaml'),
        timezone=os.getenv('LOG_TIMEZONE', 'UTC')
    )

def setup_hdfs_copy(log_manager):
    """Standard HDFS copy setup for all environments."""
    log_manager.start_hdfs_copy(
        copy_name="app_logs",
        path_patterns=["/app/logs/*.log"],
        hdfs_destination=os.getenv('HDFS_LOG_DEST', 'hdfs://cluster/logs/'),
        copy_interval=int(os.getenv('HDFS_COPY_INTERVAL', '60'))
    )

# Usage in your application
def main():
    log_manager = create_log_manager()
    setup_hdfs_copy(log_manager)  # Coordinates automatically
    
    logger = log_manager.get_logger("app")
    logger.info("Application started")
```

### 2. Container/Pod Configuration

```yaml
# Docker Compose example
version: '3.8'
services:
  coordinator:
    image: myapp
    environment:
      - LOG_LEVEL=INFO
      - HDFS_LOG_DEST=hdfs://namenode:9000/logs/
    # HDFS copy enabled by default
    
  workers:
    image: myapp
    environment:
      - LOG_LEVEL=INFO
      - DISABLE_HDFS_COPY=true  # Key difference
    deploy:
      replicas: 5
```

### 3. Node-Specific Logging

```python
import socket
import os

def setup_distributed_logging():
    """Setup logging with node-specific paths to avoid conflicts."""
    log_manager = LogManager()
    
    # Create unique log paths per node
    node_id = socket.gethostname()
    worker_id = os.getenv('WORKER_ID', '0')
    
    # Configure local logging
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
```

### 4. Health Checks

```python
def health_check():
    """Check if logging coordination is working correctly."""
    log_manager = LogManager()
    
    status = log_manager.get_hdfs_copy_status()
    operations = log_manager.list_hdfs_copy_operations()
    
    if status['hdfs_copy_enabled']:
        if len(operations) == 0:
            return {"status": "warning", "message": "HDFS copy enabled but no operations running"}
        else:
            return {"status": "healthy", "message": f"{len(operations)} HDFS operations active"}
    else:
        return {"status": "healthy", "message": "HDFS copy disabled (worker node)"}
```

---

## Monitoring and Debugging

### Check Coordination Status

```python
# Get current status
log_manager = LogManager()
status = log_manager.get_hdfs_copy_status()

print(f"HDFS copy enabled: {status['hdfs_copy_enabled']}")
print(f"Reason: {status['reason']}")
print(f"Environment variables: {status['environment_variable']}")
```

### Expected Log Messages

**Coordinator Node:**
```
HDFS copy enabled (default behavior)
Started HDFS copy operation 'app_logs' with 60s interval.
```

**Worker Node:**
```
HDFS copy disabled via DISABLE_HDFS_COPY environment variable
HDFS copy operation 'app_logs' skipped: disabled in distributed system environment
```

### Test Coordination

```python
import os

def test_coordination():
    """Verify coordination is working correctly."""
    
    # Test worker behavior
    os.environ['DISABLE_HDFS_COPY'] = 'true'
    worker_manager = LogManager()
    assert worker_manager._hdfs_copy_enabled == False
    print("✅ Worker coordination working")
    
    # Test coordinator behavior
    del os.environ['DISABLE_HDFS_COPY']
    coordinator_manager = LogManager()
    assert coordinator_manager._hdfs_copy_enabled == True
    print("✅ Coordinator coordination working")
    
    print("✅ All coordination tests passed!")

test_coordination()
```

---

## Troubleshooting

### HDFS Copy Not Working on Coordinator

**Check:**
```bash
# Verify no disable flags are set
env | grep -i hdfs

# Expected: No DISABLE_HDFS_COPY variable, or it's set to "false"
```

**Fix:**
```bash
# Remove disable flag
unset DISABLE_HDFS_COPY
```

### Multiple Nodes Copying (Race Conditions)

**Check:**
```bash
# On worker nodes, verify disable flag is set
echo $DISABLE_HDFS_COPY
# Expected: "true"
```

**Fix:**
```bash
# On ALL worker nodes
export DISABLE_HDFS_COPY=true
```

### Environment Variables Not Working

**Debug steps:**
```python
# Check environment detection
import os
print("Environment variables:")
for key, value in os.environ.items():
    if 'HDFS' in key or 'DISABLE' in key:
        print(f"  {key}={value}")

# Test manual override
os.environ['DISABLE_HDFS_COPY'] = 'true'
log_manager = LogManager()
print(f"HDFS enabled: {log_manager._hdfs_copy_enabled}")  # Should be False
```

### Container/Pod Issues

**Kubernetes:**
```bash
# Check pod environment
kubectl exec pod-name -- env | grep HDFS

# Expected on worker pods:
# DISABLE_HDFS_COPY=true
```

**Docker:**
```bash
# Check container environment
docker exec container-name env | grep HDFS
```

### Testing in Development

```python
# Simulate distributed environment locally
def test_distributed_locally():
    import subprocess
    import sys
    
    # Test coordinator
    result = subprocess.run([
        sys.executable, '-c', 
        'from my_app import LogManager; lm = LogManager(); print(f"Coordinator enabled: {lm._hdfs_copy_enabled}")'
    ], capture_output=True, text=True)
    print(result.stdout)
    
    # Test worker
    result = subprocess.run([
        sys.executable, '-c',
        'from my_app import LogManager; lm = LogManager(); print(f"Worker enabled: {lm._hdfs_copy_enabled}")'
    ], env={**os.environ, 'DISABLE_HDFS_COPY': 'true'}, capture_output=True, text=True)
    print(result.stdout)

test_distributed_locally()
```

---

## Summary

LogManager's distributed coordination provides a **simple, universal solution** for HDFS copy coordination:

### What You Get
- ✅ **Zero code changes** required in your application
- ✅ **Universal compatibility** with any distributed system
- ✅ **Simple environment variable control**
- ✅ **Automatic conflict prevention**
- ✅ **Easy deployment and configuration**

### What You Need to Do
1. **Coordinators**: Deploy normally (HDFS copy enabled by default)
2. **Workers**: Set `DISABLE_HDFS_COPY=true` environment variable
3. **Deploy**: Your existing code works automatically

### Key Takeaway
The coordination is **environment-driven** and **system-agnostic**, making it effortless to integrate with any distributed computing platform while maintaining full functionality for standalone deployments.
