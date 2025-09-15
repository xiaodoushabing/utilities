"""
Loguru TaskManager - A comprehensive task-aware logging manager built on top of Loguru.

This package provides task-aware logging with shared handlers across tasks,
designed as a single logger instance throughout the application lifecycle.
"""

from .logging import LogManager
from .file_io import FileIOInterface as FileIO

__all__ = [
    "LogManager",
    "FileIO"
]