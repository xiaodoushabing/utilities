"""
Loguru TaskManager - A comprehensive task-aware logging manager built on top of Loguru.

This package provides task-aware logging with shared handlers across tasks,
designed as a single logger instance throughout the application lifecycle.
"""

from .main.logger import LogManager

__all__ = ["LogManager"]