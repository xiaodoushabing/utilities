"""
Shared pytest fixtures for LogManager test suite.

This file centralizes common test setup and teardown logic to eliminate
duplication across test files. All fixtures defined here are automatically
available to all test files in this directory.
"""

import pytest
import tempfile
import yaml
import shutil
from unittest.mock import patch, MagicMock, mock_open

from src import LogManager


# ========================================================================================
# TEMPORARY DIRECTORY FIXTURES
# ========================================================================================

@pytest.fixture
def temp_dir():
    """
    🗂️ Creates a temporary folder for testing.
    
    - Auto-cleanup after test completes
    - Safe place for test files
    
    Usage: def test_something(temp_dir):
           file_path = os.path.join(temp_dir, 'test.txt')
    """
    temp_path = tempfile.mkdtemp()                  # 📁 Create the temporary directory
    yield temp_path                                 # 🎁 Give the path to your test
    shutil.rmtree(temp_path, ignore_errors=True)    # 🧹 Clean up after test completes


# ========================================================================================
# MOCK LOGGER FIXTURES
# ========================================================================================

@pytest.fixture
def mock_logger():
    """
    🎭 Creates a fake logger for testing.
    
    - Replaces real logger with safe mock
    - Pre-configured with realistic return values
    - Allows verification of logger calls
    
    Usage: def test_something(mock_logger):
           mock_logger.add.assert_called_once()
    """
    # 🔄 PATCH: Temporarily replace 'src.main.logger' with a fake object
    # patch() swaps out the real Loguru logger with a controllable test double
    with patch('src.main.logger') as mock:  # 🎭 Replace real logger with fake one
        
        # 🎪 MAGICMOCK: Fake objects that automatically provide any method/attribute
        # When code calls mock.some_method(), MagicMock pretends it exists and records the call
        
        # Set up fake return values for common logger methods
        mock.add.return_value = '123'                     # 🆔 Fake handler ID
        mock.level.return_value = MagicMock(no=20)        # 📊 Fake log level object (INFO=20)
        mock.bind.return_value = MagicMock()              # 🔗 Fake bound logger
        mock.remove.return_value = None                   # ❌ Remove does nothing
        mock.configure.return_value = None                # ⚙️ Configure does nothing
        yield mock  # 🎁 Give the fake logger to your test


# ========================================================================================
# CONFIGURATION FIXTURES
# ========================================================================================

@pytest.fixture
def default_config():
    """
    📋 Minimal config for unit testing.
    
    Has basic format for testing individual components.
    """
    return {
        'formats': {
            'simple': '{level} | {message}'
        },
        'handlers': {
            "handler_file": {
                'sink': '.test.log',
                'format': 'simple',
                'level': 'DEBUG'
            },
            "handler_console": {
                'sink': 'sys.stderr',
                'format': 'simple',
                'level': 'INFO'
            },
        },
        'loggers': {
            "logger_a": [
                {'handler': 'handler_file', 'level': 'DEBUG'},
                {'handler': 'handler_console', 'level': 'INFO'}
            ],
            "logger_b": [
                {'handler': 'handler_console', 'level': 'WARNING'}
            ]
        }
    }

# ========================================================================================
# LOGMANAGER FIXTURES
# ========================================================================================

@pytest.fixture
def log_manager(mock_logger, default_config):
    """
    🏭 Basic LogManager for unit testing.

    Uses default config - tests can create their own LogManager with
    specific configs when needed.
    
    🔍 WHAT HAPPENS HERE:
    1. LogManager() called with NO config_path parameter
    2. LogManager sets self._config_path = DEFAULT_CONFIG_PATH
    3. LogManager tries to open(DEFAULT_CONFIG_PATH, 'r')  
    4. But our mock intercepts and returns default_config instead!

    So it's reading from DEFAULT path, but getting FAKE content.
    """
    # mock_logger is needed to patch logger.remove() calls during LogManager init
    
    # 📄 MOCK FILE READING: Replace real file operations with fake data
    # patch('builtins.open') temporarily replaces Python's open() with a fake version during testing.
    with patch('builtins.open', mock_open(read_data=yaml.dump(default_config))):
        # When LogManager calls open(DEFAULT_CONFIG_PATH, 'r'), it gets our fake default_config
        # converted to YAML string instead of reading the real default config file
        return LogManager()  # ← NO config_path = uses DEFAULT_CONFIG_PATH


# ========================================================================================
# PYTEST CONFIGURATION
# ========================================================================================

# Global test markers for categorizing tests
pytestmark = pytest.mark.unit
