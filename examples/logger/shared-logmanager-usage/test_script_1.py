"""
Test Script 1 - Database Operations Demo

This script demonstrates using the global LogManager instance for database-related logging.
It shows how to:
1. Import the shared log_manager 
2. Get a specific logger from the configuration
3. Use various LogManager methods (logging, copy operations)
4. Log at different levels
"""

import time
from global_manager import log_manager

def main():
    print("=== Test Script 1: Database Operations ===")
    
    # Get a logger for database operations
    # This uses whatever logger configuration is in your YAML config
    db_logger = log_manager.get_logger("logger_a")  # Using logger_a from config
    
    # Log some database operations
    db_logger.info("Database connection initiated")
    db_logger.debug("Connecting to MySQL server at localhost:3306")
    
    # Simulate some database work
    for i in range(3):
        db_logger.info(f"Processing batch {i+1} of data")
        time.sleep(1)
    
    db_logger.warning("Database connection pool is 80% full")
    
    # Demonstrate LogManager copy functionality
    print("\n--- Starting file copy operation ---")
    try:
        log_manager.start_copy(
            copy_name="db_logs_backup",
            path_patterns=["./*.log"],  # Copy any log files in current directory
            copy_destination="./backup/",
            copy_interval=30,  # Copy every 30 seconds
            create_dest_dirs=True
        )
        db_logger.info("Database log backup copy operation started")
        print("Copy operation 'db_logs_backup' started successfully")
        
        # List active copy operations
        active_ops = log_manager.list_copy_operations()
        print(f"Active copy operations: {len(active_ops)}")
        for op in active_ops:
            print(f"  - {op['copy_name']}: {op['status']}")
            
    except Exception as e:
        db_logger.error(f"Failed to start copy operation: {e}")
        print(f"Error: {e}")
    
    # Demonstrate adding a new logger dynamically  
    print("\n--- Adding new logger dynamically ---")
    try:
        log_manager.add_logger(
            "db_performance", 
            [
                ("handler_file", {"level": "DEBUG"}),
                ("handler_console", {"level": "INFO"})
            ]
        )
        
        # Get and use the new logger
        perf_logger = log_manager.get_logger("db_performance")
        perf_logger.info("Performance monitoring logger created")
        perf_logger.debug("Query execution time: 0.045ms")
        
    except AssertionError as e:
        print(f"Logger might already exist: {e}")
        # Get existing logger
        perf_logger = log_manager.get_logger("db_performance")
        perf_logger.info("Using existing performance logger")
    
    db_logger.info("Database operations completed")
    print("\n=== Test Script 1 Completed ===\n")

if __name__ == "__main__":
    main()