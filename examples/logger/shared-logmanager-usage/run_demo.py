"""
Test Runner - Demonstrates Shared LogManager Usage

This script runs both test scripts sequentially to show how they share
the same LogManager instance and maintain state between scripts.

Run this script to see:
1. Script 1 initializes LogManager and starts operations
2. Script 2 uses the SAME LogManager instance with shared state
3. Operations started in script 1 are still available in script 2
4. Both scripts can access the same loggers and configurations
"""

import time
import sys
from pathlib import Path

# Add the examples/logger directory to the path so we can import our modules
sys.path.insert(0, str(Path(__file__).parent))

def main():
    print("=" * 60)
    print("SHARED LOGMANAGER DEMONSTRATION")
    print("=" * 60)
    print()
    print("This demo shows how multiple scripts can use the same LogManager")
    print("instance without resetting or losing configuration.")
    print()
    
    # Import and run first test script
    print("🚀 Running Test Script 1 (Database Operations)...")
    import test_script_1
    test_script_1.main()
    
    print("⏱️  Waiting 2 seconds to show continuous operations...")
    time.sleep(2)
    
    # Import and run second test script
    print("🚀 Running Test Script 2 (Web API Operations)...")
    import test_script_2
    test_script_2.main()
    
    # Final cleanup demonstration
    print("🧹 Cleanup Demonstration...")
    from global_manager import log_manager
    
    final_logger = log_manager.get_logger("logger_a")
    final_logger.info("Demo completed - showing final cleanup")
    
    # Show final state
    ops = log_manager.list_copy_operations()
    if ops:
        print(f"📋 Final state: {len(ops)} copy operations still running")
        print("   (These will be cleaned up when the program exits)")
        
        # Stop all operations for clean demo
        stopped_ops = log_manager.stop_all_copy(timeout=5, verbose=True)
        if stopped_ops:
            print(f"⚠️  Some operations took too long to stop: {stopped_ops}")
        else:
            print("✅ All copy operations stopped cleanly")
    else:
        print("📋 No copy operations running")
    
    print()
    print("=" * 60)
    print("KEY TAKEAWAYS:")
    print("=" * 60)
    print("✅ Same LogManager instance used across all scripts")
    print("✅ Loggers created in script 1 available in script 2") 
    print("✅ Copy operations started in script 1 visible in script 2")
    print("✅ Configuration shared between all scripts")
    print("✅ No 'logger reset' issues when importing in multiple scripts")
    print("✅ Thread-safe operations across scripts")
    print()
    print("🎉 Demonstration completed successfully!")

if __name__ == "__main__":
    main()