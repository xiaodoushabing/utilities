"""
Global LogManager instance - Initialize once, use everywhere.

This module creates a single LogManager instance that can be imported
and used across all scripts in your application.
"""

from utilities import LogManager

# Initialize LogManager once at module import time
log_manager = LogManager("../logger_config.yaml")

# Export it for easy access
__all__ = ["log_manager"]