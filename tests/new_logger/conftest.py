"""
Shared pytest fixtures for refactored LogManager test suite.

This file centralizes common test setup and teardown logic for the new
component-based architecture:
- LogManager (main coordinator)
- LoggingManager (logging configuration)
- CopyManager (file copying operations)
- DistributedCoordinator (distributed system coordination)
"""

import pytest
import tempfile
import yaml
import shutil
import atexit
from pathlib import Path
from unittest.mock import patch, MagicMock, mock_open

from src.main.logging import LogManager, LoggingManager, CopyManager, DistributedCoordinator


# ========================================================================================
# TEMPORARY DIRECTORY FIXTURES
# ========================================================================================

@pytest.fixture
def temp_dir():
    """
    Creates a temporary folder for testing.
    
    - Auto-cleanup after test completes
    - Safe place for test files
    
    Usage: def test_something(temp_dir):
           file_path = os.path.join(temp_dir, 'test.txt')
    """
    temp_path = tempfile.mkdtemp()
    yield temp_path
    shutil.rmtree(temp_path, ignore_errors=True)


# ========================================================================================
# MOCK LOGGER FIXTURES
# ========================================================================================

@pytest.fixture
def mock_logger():
    """
    Creates a fake logger for testing.
    
    - Replaces real logger with safe mock
    - Pre-configured with realistic return values
    - Allows verification of logger calls
    """
    with patch('src.main.logging._logging_manager.logger') as mock:
        mock.add.return_value = '123'
        mock.level.return_value = MagicMock(no=20)
        mock.bind.return_value = MagicMock()
        mock.remove.return_value = None
        mock.configure.return_value = None
        yield mock


# ========================================================================================
# CONFIGURATION FIXTURES
# ========================================================================================

@pytest.fixture
def default_config():
    """
    Minimal config for unit testing.
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
# COMPONENT FIXTURES
# ========================================================================================

@pytest.fixture
def logging_manager(mock_logger, default_config):
    """
    Basic LoggingManager for unit testing.
    """
    with patch('builtins.open', mock_open(read_data=yaml.dump(default_config))):
        manager = LoggingManager()
        yield manager
        manager.cleanup()


@pytest.fixture
def copy_manager():
    """
    CopyManager instance for testing (enabled by default).
    """
    manager = CopyManager(enabled=True)
    yield manager
    manager.cleanup()


@pytest.fixture
def distributed_coordinator():
    """
    DistributedCoordinator instance for testing.
    """
    return DistributedCoordinator()


@pytest.fixture
def log_manager(mock_logger, default_config):
    """
    Complete LogManager instance for integration testing.
    """
    # Patch atexit.register to prevent cleanup registration during tests
    with patch('atexit.register'):
        with patch('builtins.open', mock_open(read_data=yaml.dump(default_config))):
            manager = LogManager()
            yield manager
            manager._cleanup()


# ========================================================================================
# COPY TEST FIXTURES
# ========================================================================================

@pytest.fixture
def mock_file_discovery():
    """
    Mock file discovery to return predictable results for copy testing.
    """
    with patch('src.main.logging._copy_manager.glob.glob') as mock:
        yield mock


@pytest.fixture
def copy_defaults():
    """
    Default parameters for copy testing.
    """
    return {
        "copy_name": "test",
        "path_patterns": ["/tmp/*_log.txt"],
        "copy_destination": "hdfs://dest/",
        "root_dir": None,
        "copy_interval": 60,
        "create_dest_dirs": True,
        "preserve_structure": False,
    }


@pytest.fixture
def retry_config():
    """
    Default retry configuration for testing.
    """
    return {
        "max_attempts": 3,
        "wait": 5,
    }


@pytest.fixture
def sample_log_files(temp_dir):
    """
    Create sample log files for copy testing.
    """
    files = []
    for i in range(3):
        file_path = Path(temp_dir) / f"app_{i}_log.txt"
        with file_path.open('w') as f:
            f.write(f"Log content for file {i}")
        files.append(str(file_path))
    return files


@pytest.fixture
def mock_thread():
    """
    Creates a mock thread for testing threading functionality.
    """
    with patch('src.main.logging._copy_manager.threading.Thread') as mock_thread:
        mock_thread_instance = MagicMock()
        
        def create_mock_thread(*args, **kwargs):
            mock_thread_instance.name = kwargs.get('name', 'MockThread')
            mock_thread_instance.daemon = kwargs.get('daemon', False)
            mock_thread_instance.is_alive.return_value = True
            
            def mock_join(timeout=None):
                mock_thread_instance.is_alive.return_value = False
                return None
            
            mock_thread_instance.join.side_effect = mock_join
            return mock_thread_instance
        
        mock_thread.side_effect = create_mock_thread
        mock_thread.return_value = mock_thread_instance
        
        yield mock_thread


@pytest.fixture
def mock_event():
    """
    Creates a mock threading.Event for testing Event functionality.
    """
    with patch('src.main.logging._copy_manager.threading.Event') as mock_event:
        mock_event_instance = MagicMock()
        
        event_state = {'is_set': False}
        
        def mock_set():
            event_state['is_set'] = True
            
        def mock_clear():
            event_state['is_set'] = False
            
        def mock_is_set():
            return event_state['is_set']
            
        def mock_wait(timeout=None):
            return event_state['is_set']
        
        mock_event_instance.set.side_effect = mock_set
        mock_event_instance.clear.side_effect = mock_clear
        mock_event_instance.is_set.side_effect = mock_is_set
        mock_event_instance.wait.side_effect = mock_wait
        
        mock_event.return_value = mock_event_instance
        
        yield mock_event


# ========================================================================================
# SIGNAL HANDLING FIXTURES
# ========================================================================================

@pytest.fixture
def mock_signal():
    """
    Mock signal module for testing signal handling.
    """
    with patch('src.main.logging._copy_manager.signal') as mock_signal:
        mock_signal.SIGTERM = 15
        mock_signal.SIGINT = 2
        yield mock_signal
