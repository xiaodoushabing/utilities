"""
Conftest for HDFS copy tests that don't require full LogManager dependencies.
"""

import pytest
import tempfile
from pathlib import Path


@pytest.fixture
def temp_dir():
    """
    Creates a temporary directory for testing.
    
    Auto-cleanup after test completes.
    """
    with tempfile.TemporaryDirectory() as temp_path:
        yield temp_path


@pytest.fixture
def mock_logger():
    """
    Mock logger for basic tests that don't need the full LogManager.
    """
    from unittest.mock import MagicMock
    mock = MagicMock()
    mock.add.return_value = '123'
    mock.level.return_value = MagicMock(no=20)
    mock.bind.return_value = MagicMock()
    mock.remove.return_value = None
    mock.configure.return_value = None
    return mock
