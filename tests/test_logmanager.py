"""
Comprehensive test suite for LogManager class.

This file demonstrates pytest best practices and serves as a learning resource:
- Fixture usage and dependency injection
- Parametrized testing for multiple scenarios
- Test organization with classes
- Mock usage for external dependencies
- Error testing with pytest.raises

Key pytest concepts demonstrated:
📚 Fixtures: Reusable setup/teardown code (see conftest.py)
🔄 Parametrize: Run same test with different inputs
🎭 Mocking: Replace real dependencies with controllable fakes
🧪 Assertions: Verify expected behavior
"""

import pytest
import os
import sys
import yaml
from unittest.mock import patch, MagicMock

from logmanager import LogManager

pytestmark = pytest.mark.unit

# ========================================================================================
# PYTEST LEARNING TIP 💡
# Test classes group related tests together. Each method starting with 'test_' 
# becomes a separate test case. Classes help organize tests by functionality.

# PYTEST CONCEPTS DEMONSTRATED:
# 📚 Fixtures: Reusable setup/teardown code (see conftest.py)
# 🔄 Parametrize: Run same test with different inputs
# 🎭 Mocking: Replace real dependencies with controllable fakes
# 🧪 Assertions: Verify expected behavior
# ========================================================================================
class TestLogManagerBasics:
    """Basic LogManager functionality tests."""
    def test_logmanager_can_be_created(self, log_manager, default_config):
        """Test basic LogManager creation.
        
        PYTEST: 'log_manager' parameter is a fixture (dependency injection).
        pytest automatically finds and calls the fixture from conftest.py.
        """
        assert log_manager is not None
        assert hasattr(log_manager, '_handlers_map')
        assert hasattr(log_manager, '_loggers_map')
        assert hasattr(log_manager, '_config_path')
        assert hasattr(log_manager, 'config')
        # This test implicitly verifies default path behavior since log_manager 
        # fixture uses LogManager() with no config_path
        assert log_manager._config_path == LogManager.DEFAULT_CONFIG_PATH
        
        # PYTEST: The config gets processed during initialization, so we verify structure rather than exact equality
        # The raw config gets transformed (formats resolved, sinks converted, filters added)
        assert 'formats' in log_manager.config
        assert 'handlers' in log_manager.config  
        assert 'loggers' in log_manager.config
        
        # Verify the basic structure matches what we expect from default config
        assert log_manager.config['formats'] == default_config['formats']
        assert set(log_manager.config['loggers'].keys()) == set(default_config['loggers'].keys())
        assert set(log_manager.config['handlers'].keys()) == set(default_config['handlers'].keys())
    
    def test_config_loading_from_custom_path(self, mock_logger, temp_dir):
        """Test loading config from custom file path."""
        # Create a simple config file
        custom_config = {
            'formats': {'test': '{message}'},
            'handlers': {},
            'loggers': {}
            }
        config_path = os.path.join(temp_dir, 'custom.yaml')
        
        with open(config_path, 'w') as f:
            yaml.dump(custom_config, f)
        
        lm = LogManager(config_path=config_path)
        
        assert lm._config_path == config_path
        assert lm.config == custom_config

    def test_mock_logger_integration_with_logmanager(self, log_manager, mock_logger):
        """Test that LogManager operations properly use the mocked logger.
        
        PYTEST: Verify that when LogManager performs operations, it actually
        calls the mocked logger methods, proving the mock injection works.
        """
        # Reset mock to start with clean state
        mock_logger.reset_mock()
        
        # Test that get_logger calls mock.bind
        logger = log_manager.get_logger("logger_a")  # From default config
        assert logger is not None
        mock_logger.bind.assert_called_once_with(logger_name="logger_a")
        
        # Test that add_handler calls mock.add
        mock_logger.reset_mock()
        log_manager.add_handler("test_handler", {
            'sink': 'sys.stdout',
            'format': 'simple',
            'level': 'INFO'
        })
        
        # Verify mock.add was called exactly once
        mock_logger.add.assert_called_once()
        
        # Verify the handler was stored with the mocked return value
        assert log_manager._handlers_map["test_handler"]["id"] == '123'  # Mock return value
        
        # Test that remove_handler calls mock.remove
        mock_logger.reset_mock()
        log_manager.remove_handler("test_handler")
        
        # Verify mock.remove was called with the stored handler ID
        mock_logger.remove.assert_called_once_with('123')
        
        # Verify handler was removed from internal mapping
        assert "test_handler" not in log_manager._handlers_map

class TestLogManagerInitialization:
    """Test LogManager initialization and setup behavior."""
    
    def test_timezone_environment_variable_set(self, mock_logger):
        """Test that timezone is properly set in environment."""
        custom_timezone = "UTC"
        lm = LogManager(timezone=custom_timezone)
        assert os.environ["TZ"] == custom_timezone
    
    def test_default_timezone_applied(self, mock_logger):
        """Test default timezone is set when none provided."""
        lm = LogManager()
        assert os.environ["TZ"] == "Asia/Singapore"
    
    def test_atexit_cleanup_registered(self, mock_logger):
        """Test that cleanup function is registered with atexit."""
        with patch('atexit.register') as mock_atexit:
            lm = LogManager()
            mock_atexit.assert_called_once_with(lm._cleanup)
    
    def test_logger_removal_during_init(self, mock_logger):
        """Test that default logger is removed during initialization."""
        LogManager()
        mock_logger.remove.assert_called()  # Should remove default handlers

    def test_nonexistent_config_fallback_to_default(self, mock_logger):
        """Test fallback to default config when file doesn't exist."""
        lm = LogManager(config_path="nonexistent.yaml")
        assert lm._config_path == LogManager.DEFAULT_CONFIG_PATH
    
    def test_invalid_file_path_fallback(self, mock_logger, temp_dir):
        """Test fallback when config_path is a directory, not a file."""
        lm = LogManager(config_path=temp_dir)
        assert lm._config_path == LogManager.DEFAULT_CONFIG_PATH

class TestMappingCleanup:
    """Test handler-logger relationship cleanup."""
    
    @pytest.fixture
    def handler_logger_mapping(self, log_manager):
        """Create a handler-logger bidirectional mapping for testing cleanup behavior.
        
        PYTEST: Fixture that sets up the common test scenario - a handler and logger
        that reference each other bidirectionally. This eliminates duplication across
        all mapping cleanup tests.
        """
        # Add handler and logger to create bidirectional mapping
        log_manager.add_handler("test_handler", {
            'sink': 'sys.stdout', 'format': 'simple', 'level': 'INFO'
        })
        log_manager.add_logger("test_logger", [
            {'handler': 'test_handler', 'level': 'DEBUG'}
        ])
        
        # Return both names for easy reference in tests
        return {
            'handler_name': 'test_handler',
            'logger_name': 'test_logger'
        }
    
    def test_bidirectional_mapping_creation(self, handler_logger_mapping, log_manager):
        """Test that adding handler and logger creates proper bidirectional mapping."""
        handler_name = handler_logger_mapping['handler_name']
        logger_name = handler_logger_mapping['logger_name']
        
        # Verify bidirectional mapping exists
        # Side 1: _loggers_map should reference handler
        assert handler_name in [h["handler"] for h in log_manager._loggers_map[logger_name]]
        # Side 2: _handlers_map should reference logger
        assert handler_name in log_manager._handlers_map
        assert logger_name in log_manager._handlers_map[handler_name]["loggers"]
    
    def test_remove_handler_cleans_mappings(self, handler_logger_mapping, log_manager):
        """Test that removing handler cleans up references in bidirectional mapping."""
        handler_name = handler_logger_mapping['handler_name']
        logger_name = handler_logger_mapping['logger_name']
        
        # Remove handler
        log_manager.remove_handler(handler_name)
        
        # Verify bidirectional cleanup:
        # Side 1: Logger should still exist but with no handler references
        assert logger_name in log_manager._loggers_map  # Logger still exists
        remaining_handlers = [h["handler"] for h in log_manager._loggers_map[logger_name]]
        assert handler_name not in remaining_handlers  # But handler reference removed
        # Side 2: Handler should be completely removed from _handlers_map
        assert handler_name not in log_manager._handlers_map
    
    def test_remove_logger_cleans_handler_mappings(self, handler_logger_mapping, log_manager):
        """Test that removing logger cleans up handler references in bidirectional mapping."""
        handler_name = handler_logger_mapping['handler_name']
        logger_name = handler_logger_mapping['logger_name']
        
        # Remove logger
        log_manager.remove_logger(logger_name)
        
        # Verify bidirectional cleanup:
        # Side 1: Logger should be completely removed from _loggers_map
        assert logger_name not in log_manager._loggers_map
        # Side 2: Handler should still exist but with no logger references
        assert handler_name in log_manager._handlers_map  # Handler still exists
        assert logger_name not in log_manager._handlers_map[handler_name]["loggers"]  # But logger reference removed
        assert len(log_manager._handlers_map[handler_name]["loggers"]) == 0  # Handler should have no loggers

class TestCleanupBehavior:
    """Test cleanup and teardown functionality."""
    
    @pytest.fixture
    def populated_log_manager(self, log_manager):
        """Fixture that provides a LogManager with some test data for cleanup testing.
        
        PYTEST: Creates a LogManager instance with test handlers and loggers already
        configured, so cleanup tests can verify that data is properly cleared.
        """
        # Add some data first to create bidirectional mappings
        log_manager.add_handler("test_handler", {
            'sink': 'sys.stdout', 'format': 'simple', 'level': 'INFO'
        })
        log_manager.add_logger("test_logger", [
            {'handler': 'test_handler', 'level': 'DEBUG'}
        ])
        return log_manager
    
    def test_cleanup_removes_all_handlers(self, log_manager, mock_logger):
        """Test that cleanup removes all loguru handlers."""
        mock_logger.reset_mock()
        log_manager._cleanup()
        mock_logger.remove.assert_called_once()  # Should remove all handlers
    
    def test_cleanup_clears_internal_mappings(self, populated_log_manager):
        """Test that cleanup clears internal data structures."""
        # Cleanup the populated log manager
        populated_log_manager._cleanup()
        
        # Verify both sides of bidirectional mapping are cleared
        assert len(populated_log_manager._handlers_map) == 0
        assert len(populated_log_manager._loggers_map) == 0
    
class TestHandlerFilterBehavior:
    """Test handler filter function creation and behavior."""
    
    @pytest.fixture
    def handler_logger_setup(self, log_manager):
        """Setup handler and logger for filter testing.
        
        PYTEST: Fixture that provides pre-configured handler and logger for all filter tests.
        This eliminates duplication across multiple test methods.
        """
        # Add a handler with ERROR level threshold
        log_manager.add_handler("test_handler", {
            'sink': 'sys.stdout', 'format': 'simple', 'level': 'ERROR'
        })
        
        # Add a logger that uses this handler with DEBUG level
        log_manager.add_logger("test_logger", [
            {'handler': 'test_handler', 'level': 'DEBUG'}
        ])
        
        # Return the filter function for testing
        return log_manager._make_handler_filter("test_handler")
    
    @pytest.mark.parametrize("logger_name,record_level,expected_result,test_description", [
        # Test correct logger with various levels against ERROR (40) threshold defined by handler
        ("test_logger", 50, True, "allows correct logger with CRITICAL level (50 >= 40)"),
        ("test_logger", 40, True, "allows correct logger with ERROR level (40 >= 40)"),
        ("test_logger", 30, False, "blocks correct logger with WARNING level (30 < 40)"),
        ("test_logger", 20, False, "blocks correct logger with INFO level (20 < 40)"),
        ("test_logger", 10, False, "blocks correct logger with DEBUG level (10 < 40)"),
        # Test wrong logger (should always block regardless of level)
        ("different_logger", 50, False, "blocks logs from unassociated loggers"),
        ("different_logger", 10, False, "blocks logs from unassociated loggers"),
        # Edge cases
        ("", 50, False, "blocks empty logger name"),
    ])
    def test_handler_filter_behavior(self, handler_logger_setup, mock_logger, 
                                   logger_name, record_level, expected_result, test_description):
        """Test handler filter behavior with various logger names and levels.
        
        PYTEST: Parametrized test covering all filter scenarios against handler's configured threshold.
        The handler is configured with ERROR level (40), so only ERROR and above should pass.
        """
        filter_func = handler_logger_setup
        
        # Create mock record
        mock_record = {
            "extra": {"logger_name": logger_name},
            "level": MagicMock(no=record_level)
        }
        
        # Mock logger.level() to return ERROR level (40) - this should match handler config
        mock_logger.level.return_value = MagicMock(no=40)
        
        # Test filter behavior
        result = filter_func(mock_record)
        assert result == expected_result, f"Filter {test_description}"
class TestHandlerManagement:
    """Test handler CRUD operations."""
    
    @pytest.fixture
    def basic_handler_config(self):
        """Shared handler configuration for tests.
        
        PYTEST: Class-level fixture - only available to methods in this class.
        Fixtures can be at function, class, module, or session scope.
        """
        return {
            'sink': 'sys.stdout',
            'format': 'simple',
            'level': 'INFO'
        }
    
    def test_add_handler(self, log_manager, mock_logger, basic_handler_config):
        """Test adding a new handler."""
        handler_name = 'test_handler'
        
        mock_logger.reset_mock()
        log_manager.add_handler(handler_name, basic_handler_config)
        
        # Verify handler was added to internal mapping
        assert handler_name in log_manager._handlers_map
        assert log_manager._handlers_map[handler_name]["id"] == mock_logger.add.return_value
        
        # Verify logger.add() was called exactly once
        mock_logger.add.assert_called_once()
    
    def test_update_handler(self, log_manager, mock_logger, basic_handler_config):
        """Test updating an existing handler."""
        handler_name = 'update_test_handler'
        
        # First add a handler
        log_manager.add_handler(handler_name, basic_handler_config)
        
        # Now update it
        mock_logger.reset_mock()
        updated_config = {
            'sink': 'sys.stderr',
            'format': 'Updated {level} | {message}',
            'level': 'DEBUG'
        }
        log_manager.update_handler(handler_name, updated_config)
        
        # Verify old handler removed and new one added
        mock_logger.remove.assert_called_once_with('123')  # Mock ID
        mock_logger.add.assert_called_once()
        
        # Verify new handler ID stored
        assert log_manager._handlers_map[handler_name]["id"] == mock_logger.add.return_value
    
    def test_remove_handler(self, log_manager, mock_logger, basic_handler_config):
        """Test removing an existing handler."""
        handler_name = 'remove_test_handler'
        
        # First add a handler
        log_manager.add_handler(handler_name, basic_handler_config)
        
        # Now remove it
        mock_logger.reset_mock()
        log_manager.remove_handler(handler_name)
        
        # Verify handler removed from logger and internal mapping
        mock_logger.remove.assert_called_once_with('123')  # Mock ID
        assert handler_name not in log_manager._handlers_map


class TestHandlerValidation:
    """Test handler configuration validation and transformations."""
    
    @pytest.mark.parametrize("missing_key", ['sink', 'level', 'format'])
    def test_missing_required_keys(self, log_manager, missing_key):
        """Test that missing required keys are caught.
        """
        handler_config = {
            'sink': 'sys.stdout',
            'format': 'simple', 
            'level': 'INFO'
        }
        del handler_config[missing_key]
        
        # PYTEST: pytest.raises() context manager catches expected exceptions
        with pytest.raises(AssertionError, match=f"must have a '{missing_key}' key"):
            log_manager.add_handler('invalid', handler_config)
    
    @pytest.mark.parametrize("level_input,expected_level", [
        ('debug', 'DEBUG'), 
        ('INFO', 'INFO'),
        ('Error', 'ERROR'),
    ])
    def test_level_normalization(self, log_manager, level_input, expected_level, mock_logger):
        """Test that levels are normalized to uppercase.
        """
        handler_config = {
            'sink': 'sys.stdout',
            'format': 'simple',
            'level': level_input
        }
        
        mock_logger.reset_mock()
        log_manager.add_handler('test_level', handler_config)
        
        # PYTEST: Inspect mock call arguments to verify behavior
        call_args = mock_logger.add.call_args[1]  # [1] gets keyword arguments
        assert call_args['level'] == expected_level
    
    @pytest.mark.parametrize("sink_input,expected_sink", [
        ('sys.stdout', sys.stdout),
        ('sys.stderr', sys.stderr),
        ('./test.log', './test.log'),
    ])
    def test_sink_conversion(self, log_manager, sink_input, expected_sink, mock_logger):
        """Test that sinks are properly converted."""
        handler_config = {
            'sink': sink_input,
            'format': 'simple',
            'level': 'INFO'
        }
        
        mock_logger.reset_mock()
        log_manager.add_handler('test_sink', handler_config)
        
        call_args = mock_logger.add.call_args[1]
        assert call_args['sink'] == expected_sink
    
    def test_format_reference_conversion(self, log_manager, mock_logger):
        """Test that format references are properly converted from config."""
        # PYTEST: Test format reference lookup from config's 'formats' section
        handler_config = {
            'sink': 'sys.stdout',
            'format': 'simple',  # This should reference the 'simple' format in config
            'level': 'INFO'
        }
        
        mock_logger.reset_mock()
        log_manager.add_handler('test_format_ref', handler_config)
        
        call_args = mock_logger.add.call_args[1]
        # Should be converted to the actual format string from config
        assert call_args['format'] == log_manager.config['formats']['simple']
    
    def test_custom_format_passthrough(self, log_manager, mock_logger):
        """Test that custom format strings are passed through unchanged."""
        # PYTEST: Test custom format (not in config) is used as-is
        custom_format = '{time} | {level} | {message}'
        handler_config = {
            'sink': 'sys.stdout',
            'format': custom_format,  # Custom format, not in config
            'level': 'INFO'
        }
        
        mock_logger.reset_mock()
        log_manager.add_handler('test_custom_format', handler_config)
        
        call_args = mock_logger.add.call_args[1]
        # Custom format should be passed through unchanged
        assert call_args['format'] == custom_format


class TestLoggerManagement:
    """Test logger CRUD operations and retrieval."""
    
    @pytest.fixture 
    def sample_logger_configs(self):
        """Sample logger configurations for testing.
        
        PYTEST: Returns data in the actual format used by add_logger method:
        list[dict] where each dict has 'handler' and 'level' keys
        """
        return {
            'single_logger': [{'handler': 'handler_console', 'level': 'INFO'}],
            'multi_logger': [
                {'handler': 'handler_console', 'level': 'INFO'},
                {'handler': 'handler_file', 'level': 'DEBUG'}
            ]
        }
    
    def test_add_logger(self, log_manager, sample_logger_configs):
        """Test adding a new logger."""
        logger_name = 'test_logger'
        config = sample_logger_configs['single_logger']  # Get config from fixture
        
        log_manager.add_logger(logger_name, config)
        
        # Verify logger was added to internal mapping
        assert logger_name in log_manager._loggers_map  
        assert log_manager._loggers_map[logger_name] == config
        handler_name = config[0]['handler']  # Get first handler name
        assert handler_name in log_manager._handlers_map
        # Verify the handler's loggers mapping contains this logger with correct level
        assert logger_name in log_manager._handlers_map[handler_name]["loggers"]
        assert log_manager._handlers_map[handler_name]["loggers"][logger_name]["level"] == config[0]['level'].upper()

    def test_get_logger_added(self, log_manager, mock_logger, sample_logger_configs):
        """Test retrieving a logger that was added."""
        logger_name = 'retrieval_test_logger'
        config = sample_logger_configs['single_logger']

        # First add a logger
        log_manager.add_logger(logger_name, config)
        
        # Now retrieve it
        mock_logger.reset_mock()
        logger = log_manager.get_logger(logger_name)
        
        # Verify logger.bind() was called with correct name
        assert logger is not None
        mock_logger.bind.assert_called_with(logger_name=logger_name)
    
    def test_get_logger_from_config(self, log_manager, mock_logger):
        """Test retrieving a logger that exists in the default configuration."""
        # Get the first logger name from the actual config (dynamic, not hardcoded)
        logger_names = list(log_manager.config["loggers"].keys())
        assert len(logger_names) > 0, "Config should have at least one logger defined"
        
        logger_name = logger_names[0]  # Get first logger from config
        existing_logger = log_manager.get_logger(logger_name)
        assert existing_logger is not None
        mock_logger.bind.assert_called_with(logger_name=logger_name)

    def test_update_logger(self, log_manager, sample_logger_configs):
        """Test updating an existing logger."""
        logger_name = 'update_test_logger'
        original_config = sample_logger_configs['single_logger']
        updated_config = sample_logger_configs['multi_logger']
        
        # First add a logger
        log_manager.add_logger(logger_name, original_config)
        
        # Now update it
        log_manager.update_logger(logger_name, updated_config)
        
        # Verify logger was updated
        assert log_manager._loggers_map[logger_name] == updated_config
        assert len(log_manager._loggers_map[logger_name]) == len(updated_config)
    
    def test_remove_logger(self, log_manager, sample_logger_configs):
        """Test removing an existing logger."""
        logger_name = 'remove_test_logger'
        config = sample_logger_configs['single_logger']

        # First add a logger
        log_manager.add_logger(logger_name, config)
        
        # Now remove it
        log_manager.remove_logger(logger_name)
        
        # Verify logger was removed from internal mapping
        assert logger_name not in log_manager._loggers_map
    
    def test_duplicate_logger_rejection(self, log_manager, sample_logger_configs):
        """Test that duplicate logger names are rejected."""
        config = sample_logger_configs['single_logger']
        log_manager.add_logger('duplicate', config)
        
        with pytest.raises(AssertionError, match="already exists"):
            log_manager.add_logger('duplicate', config)


# ========================================================================================
# PYTEST LEARNING TIP 💡  
# Grouping similar error tests together makes it easy to see all failure scenarios
# and ensures consistent error handling across the codebase.
# ========================================================================================

class TestDuplicateEntityOperations:
    """Test duplicate entity rejection for handlers and loggers."""
    
    @pytest.mark.parametrize("entity_type,add_method,config", [
        ('handler', 'add_handler', {'sink': 'sys.stdout', 'format': 'simple', 'level': 'INFO'}),
        ('logger', 'add_logger', [{'handler': 'handler_console', 'level': 'INFO'}]),
    ])
    def test_duplicate_entity_rejection(self, log_manager, entity_type, add_method, config):
        """Test that duplicate entity names are rejected.
        
        PYTEST: Parametrized test covering both handlers and loggers in one test.
        This ensures consistent duplicate rejection behavior across entity types.
        """
        entity_name = f'duplicate_{entity_type}'
        method = getattr(log_manager, add_method)
        
        # First addition should succeed
        method(entity_name, config)
        
        # Second addition should fail
        with pytest.raises(AssertionError, match="already exists"):
            method(entity_name, config)


# ========================================================================================
# PYTEST LEARNING TIP 💡  
# Error testing is crucial! Use pytest.raises() to verify that your code properly
# handles invalid inputs and edge cases. This prevents silent failures.
# ========================================================================================

class TestNonexistentEntityOperations:
    """Test operations on nonexistent entities raise appropriate errors."""
    
    @pytest.mark.parametrize("method_name,args", [
        # Test operations on nonexistent handlers  
        ('remove_handler', ['nonexistent_handler']),
        ('update_handler', ['nonexistent_handler', {'sink': 'test', 'format': 'simple', 'level': 'INFO'}]),
        # Test operations on nonexistent loggers
        ('get_logger', ['nonexistent_logger']),
        ('update_logger', ['nonexistent_logger', []]),
        ('remove_logger', ['nonexistent_logger']),
    ])
    def test_nonexistent_entity_operations(self, log_manager, method_name, args):
        """Test operations on nonexistent entities raise appropriate errors.
        
        PYTEST: Parametrized test efficiently covers multiple error scenarios.
        This ensures consistent error handling when entities don't exist.
        
        PYTHON *args EXPLANATION 🔍:
        The *args unpacking operator takes a list and spreads it as separate arguments.
        
        Example with update_handler:
        - args = ['nonexistent_handler', {'sink': 'test', 'format': 'simple', 'level': 'INFO'}]
        - method(*args) becomes: method('nonexistent_handler', {'sink': 'test', 'format': 'simple', 'level': 'INFO'})
        - This is equivalent to: log_manager.update_handler('nonexistent_handler', {'sink': 'test', 'format': 'simple', 'level': 'INFO'})
        
        Without *args, you'd have to write separate test methods for each operation!
        """
        method = getattr(log_manager, method_name)  # Get method by string name
        
        with pytest.raises(AssertionError, match="does not exist"):
            method(*args)  # Unpack arguments list
    
    def test_logger_with_nonexistent_handler_reference(self, log_manager):
        """Test adding logger that references nonexistent handlers."""
        # This tests whether the system allows "forward references" to handlers
        logger_config = [{'handler': 'future_handler', 'level': 'INFO'}]
        
        # This should NOT be allowed - validation should happen at config-time to prevent silent failures
        with pytest.raises(KeyError, match="does not exist"):
            log_manager.add_logger('forward_ref_logger', logger_config)


