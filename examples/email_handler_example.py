"""
Simple Email Handler Usage Example

This script shows how to use the simplified email handler.
"""

import sys
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent / "src"
sys.path.insert(0, str(project_root))

from main.logging._logging_manager import LoggingManager


def main():
    """Main example function."""
    
    print("Email Handler Usage Example")
    print("=" * 40)
    
    # Use the simplified email configuration
    config_path = project_root.parent / "src" / "main" / "logging" / "email_example_config.yaml"
    
    print(f"Using config: {config_path}")
    print("NOTE: Update the email credentials in the config file to test with real emails.")
    print()
    
    try:
        # Initialize the logging manager
        manager = LoggingManager(str(config_path))
        
        # Get a logger
        logger = manager.get_logger("critical_logger")
        
        print("Testing log messages (update config with real credentials to send emails):")
        
        # These will only go to file and console (not email due to level filtering)
        logger.debug("Debug message - file and console only")
        logger.info("Info message - console only") 
        logger.warning("Warning message - console only")
        
        # This will trigger email (ERROR level) if properly configured
        logger.error("This is a critical error that will be emailed!")
        logger.critical("This is a critical message that will also be emailed!")
        
        print("\nTesting the simple send_email method:")
        
        # Create a test log file
        test_file = "test_log.txt"
        with open(test_file, 'w') as f:
            f.write("Sample log content\nError occurred at 12:34:56\nSystem status: CRITICAL")
        
        try:
            # Example: Send log file as attachment using the simple API
            print(f"Sending {test_file} as attachment...")
            manager.send_email(
                file_path=test_file,
                to_emails=["admin@example.com", "developer@example.com"]
                # Uses first available email handler automatically
            )
            print("✓ send_email method called successfully!")
            
        except AssertionError as e:
            print(f"Expected error (no real email config): {e}")
        
        # Clean up test file
        import os
        if os.path.exists(test_file):
            os.remove(test_file)
            
    except Exception as e:
        print(f"Error in example: {e}")
        import traceback
        traceback.print_exc()
        
    finally:
        # Always cleanup
        if 'manager' in locals():
            manager.cleanup()


def show_real_usage():
    """Show how to use with real email configuration."""
    print("\n" + "=" * 50)
    print("Real Usage Example:")
    print("=" * 50)
    
    print("""
1. Update the email_example_config.yaml with your credentials:

handlers:
  handler_email:
    sink: "email"
    format: "email"
    level: "ERROR"
    email_config:
      to_emails:
        - "your-admin@yourdomain.com"  # <-- Update these
      subject_prefix: "[PROD LOG]"
      smtp_server: "smtp.gmail.com"    # <-- Your SMTP server
      smtp_port: 587
      username: "yourapp@gmail.com"    # <-- Your email
      password: "your_app_password"    # <-- Your app password
      from_email: "yourapp@gmail.com"  # <-- Your email

2. Use in your application:

from main.logging._logging_manager import LoggingManager

# Initialize with your config
manager = LoggingManager('path/to/your_email_config.yaml')
logger = manager.get_logger('my_logger')

# Log messages (ERROR and above will be emailed immediately)
logger.error("Database connection failed!")
logger.critical("System is down!")

# Send log files as attachments
manager.send_email('app.log', ['admin@company.com'])

3. Key Benefits:
✓ Simple configuration (only required fields)
✓ Immediate email sending (no complex batching)
✓ Clean API: send_email(file_path, emails)
✓ Uses reliable notifiers library
✓ Easy to understand and maintain
""")


if __name__ == "__main__":
    main()
    show_real_usage()