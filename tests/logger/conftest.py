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
from pathlib import Path
from unittest.mock import patch, MagicMock, mock_open

from utilities import LogManager
from utilities.logger import LoggingManager, CopyManager, DistributedCoordinator


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
    temp_path = tempfile.mkdtemp()                  # Create the temporary directory
    yield temp_path                                 # Give the path to your test
    shutil.rmtree(temp_path, ignore_errors=True)    # Clean up after test completes


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
    
    Usage: def test_something(mock_logger):
           mock_logger.add.assert_called_once()
    """
    # PATCH: Temporarily replace 'logmanager.logger' with a fake object
    # patch() swaps out the real Loguru logger with a controllable test double
    with patch('utilities.logger._logging_manager.logger') as mock:  # Replace real logger with fake one
        
        # MAGICMOCK: Fake objects that automatically provide any method/attribute
        # When code calls mock.some_method(), MagicMock pretends it exists and records the call
        
        # Set up fake return values for common logger methods
        mock.add.return_value = '123'                     # Fake handler ID
        mock.level.return_value = MagicMock(no=20)        # Fake log level object (INFO=20)
        mock.bind.return_value = MagicMock()              # Fake bound logger
        mock.remove.return_value = None                   # Remove does nothing
        mock.configure.return_value = None                # ⚙️onfigure does nothing
        yield mock  # Give the fake logger to your test


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
def logging_manager(mock_logger, default_config):
    """
    🏭 Basic LoggingManager for unit testing.

    Uses default config - tests can create their own LoggingManager with
    specific configs when needed.

    🔍 WHAT HAPPENS HERE:
    1. LoggingManager() called with NO config_path parameter
    2. LoggingManager sets self._config_path = DEFAULT_CONFIG_PATH
    3. LoggingManager tries to open(DEFAULT_CONFIG_PATH, 'r')  
    4. But our mock intercepts and returns default_config instead!

    So it's reading from DEFAULT path, but getting FAKE content.
    """
    # mock_logger is needed to patch logger.remove() calls during LoggingManager init
    with patch('builtins.open', mock_open(read_data=yaml.dump(default_config))):
        return LoggingManager()  # ← NO config_path = uses DEFAULT_CONFIG_PATH

# ========================================================================================
# COPY TEST FIXTURES
# ========================================================================================

@pytest.fixture
def mock_file_discovery():
    """
    Mock file discovery to return predictable results for copy testing.
    
    - Replaces LogManager._discover_files_to_copy with controllable mock
    - Allows tests to control which files are "discovered"
    - Pre-configured with common return values
    
    Usage: def test_something(mock_file_discovery):
           mock_file_discovery.return_value = ["/tmp/file1.log", "/tmp/file2.log"]
    """
    with patch.object(LogManager, '_discover_files_to_copy') as mock:
        yield mock

@pytest.fixture
def copy_defaults():
    """
    📦 Default parameters for copy testing.
    
    Provides a complete set of valid default parameters that can be 
    merged with test-specific invalid parameters to test validation.
    
    Usage: 
        params = {**copy_defaults, **invalid_params}
        log_manager.start_copy(**params)
    """
    return {
        "copy_name": "test",
        "path_patterns": ["/tmp/*_log.txt"],
        "copy_destination": "hdfs://dest/",
        "root_dir": None,
        "copy_interval": 60,
        "create_dest_dirs": True,
        "preserve_structure": False,
        "max_retries": 3,
        "retry_delay": 5,
    }

@pytest.fixture
def sample_log_files(temp_dir):
    """
    Create sample log files for copy testing.
    
    Creates 3 test log files with content.

    Usage: files = _discover_files_to_copy(temp_dir)
           assert len(files) == 3
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
    
    - Replaces real threading.Thread with controllable mock
    - Pre-configured with realistic behavior
    - Allows verification of thread creation and method calls
    
    Usage: def test_something(mock_thread):
           mock_thread.start.assert_called_once()
           mock_thread.join.assert_called_once()
    """
    with patch('utilities.logger._copy_manager.threading.Thread') as mock_thread:
        # Create a single mock instance that will be returned for all calls
        mock_thread_instance = MagicMock()
        
        def create_mock_thread(*args, **kwargs):
            # Set the thread name from kwargs if provided, otherwise use default
            mock_thread_instance.name = kwargs.get('name', 'MockThread')
            mock_thread_instance.daemon = kwargs.get('daemon', False)
            mock_thread_instance.is_alive.return_value = True
            
            def mock_join(timeout=None):
                mock_thread_instance.is_alive.return_value = False
                return None
            
            mock_thread_instance.join.side_effect = mock_join
            
            return mock_thread_instance
        
        mock_thread.side_effect = create_mock_thread
        mock_thread.return_value = mock_thread_instance  # Also set return_value for compatibility
        
        yield mock_thread

@pytest.fixture
def mock_event():
    """
    Creates a mock threading.Event for testing Event functionality.
    
    - Replaces real threading.Event with controllable mock
    - Pre-configured with realistic behavior for Event instances
    - Allows verification of event creation and method calls
    
    Usage: def test_something(mock_event):
           mock_event.return_value.set.assert_called_once()
           mock_event.return_value.wait.assert_called_once()
    """
    with patch('utilities.logger._copy_manager.threading.Event') as mock_event:
        mock_event_instance = MagicMock()
        
        # Create a state variable to track if the event is set
        event_state = {'is_set': False}
        
        def mock_set():
            event_state['is_set'] = True
            
        def mock_clear():
            event_state['is_set'] = False
            
        def mock_is_set():
            return event_state['is_set']
            
        def mock_wait(timeout=None):
            # In real Event, wait() returns True if event is set, False if timeout
            return event_state['is_set']
        
        # Configure realistic behavior for Event methods
        mock_event_instance.set.side_effect = mock_set
        mock_event_instance.clear.side_effect = mock_clear
        mock_event_instance.is_set.side_effect = mock_is_set
        mock_event_instance.wait.side_effect = mock_wait
        
        mock_event.return_value = mock_event_instance
        
        yield mock_event
