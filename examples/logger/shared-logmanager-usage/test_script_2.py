"""
Test Script 2 - Web API Demo

This script demonstrates using the SAME global LogManager instance from a different script.
It shows how:
1. The LogManager state is preserved across imports
2. Loggers configured in script 1 are available in script 2
3. Copy operations started in script 1 are still running
4. Multiple scripts can safely use the same LogManager instance
"""

import time
from global_manager import log_manager

def simulate_api_requests():
    """Simulate handling API requests"""
    
    # Get the same logger that was configured in the shared LogManager
    api_logger = log_manager.get_logger("logger_b")  # Using logger_b from config
    
    api_endpoints = [
        "/api/users",
        "/api/orders", 
        "/api/products",
        "/api/auth/login"
    ]
    
    for i, endpoint in enumerate(api_endpoints):
        # Simulate different response scenarios
        if i == 1:
            api_logger.warning(f"Slow response for {endpoint} - 2.5s")
        elif i == 3:
            api_logger.error(f"Authentication failed for {endpoint}")
        else:
            api_logger.info(f"Successfully handled request to {endpoint}")
        
        time.sleep(0.5)

def check_shared_resources():
    """Demonstrate that we're using the same LogManager instance"""
    
    print("=== Checking Shared LogManager Resources ===")
    
    # Check if copy operations from script 1 are still running
    active_ops = log_manager.list_copy_operations()
    print(f"Copy operations from other scripts still active: {len(active_ops)}")
    for op in active_ops:
        print(f"  - {op['copy_name']}: {op['status']}")
    
    # Try to use the logger that script 1 created dynamically
    try:
        perf_logger = log_manager.get_logger("db_performance")
        perf_logger.info("API script accessing database performance logger created by script 1")
        print("✓ Successfully accessed logger created by script 1")
    except AssertionError:
        print("✗ Logger from script 1 not found (this shouldn't happen)")
    
    # Show that we can access the same configuration
    config_keys = list(log_manager.config.keys())
    print(f"Shared configuration sections: {config_keys}")

def demonstrate_promtail():
    """Demonstrate Promtail functionality"""
    
    api_logger = log_manager.get_logger("logger_a")
    
    print("\n--- Testing Promtail Integration ---")
    try:
        # Start promtail with some configuration
        promtail_config = {
            "instance_name": "api_server",
            "target_paths": ["./*.log"],
            "log_level": "INFO",
            "static_labels": {
                "service": "api",
                "environment": "development"
            }
        }
        
        log_manager.start_promtail(promtail_config)
        api_logger.info("Promtail agent started for log forwarding")
        print("Promtail started successfully")
        
        # Log some messages that Promtail will collect
        api_logger.info("API server started on port 8080")
        api_logger.info("Health check endpoint available at /health")
        
    except Exception as e:
        api_logger.warning(f"Promtail not available: {e}")
        print(f"Promtail warning: {e}")

def main():
    print("=== Test Script 2: Web API Operations ===")
    
    # Get API logger
    api_logger = log_manager.get_logger("logger_a")  # Using same logger as script 1
    api_logger.info("Web API module loaded")
    
    # Check shared resources first
    check_shared_resources()
    
    # Simulate API work
    print("\n--- Simulating API Requests ---")
    simulate_api_requests()
    
    # Demonstrate Promtail
    demonstrate_promtail()
    
    # Start another copy operation to show multiple copies can run
    print("\n--- Starting additional copy operation ---")
    try:
        log_manager.start_copy(
            copy_name="api_access_logs",
            path_patterns=["./*.log"],
            copy_destination="./api_backups/",
            copy_interval=45,
            create_dest_dirs=True
        )
        api_logger.info("API access log backup started")
        print("Additional copy operation started")
        
    except ValueError as e:
        print(f"Copy operation info: {e}")
    
    # Show final state
    print("\n--- Final State Check ---")
    active_ops = log_manager.list_copy_operations()
    print(f"Total active copy operations: {len(active_ops)}")
    for op in active_ops:
        print(f"  - {op['copy_name']}: {op['status']}")
    
    # Trigger immediate copy for demonstration
    if active_ops:
        print("\n--- Triggering immediate copy ---")
        try:
            log_manager.trigger_copy_now()  # Trigger all copy operations
            api_logger.info("Immediate copy triggered for all operations")
            print("Immediate copy triggered successfully")
        except Exception as e:
            print(f"Copy trigger error: {e}")
    
    api_logger.info("API operations completed")
    print("\n=== Test Script 2 Completed ===\n")

if __name__ == "__main__":
    main()