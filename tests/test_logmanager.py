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

from src import LogManager


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


# ========================================================================================
# PYTEST LEARNING TIP 💡
# Integration tests verify that multiple components work together correctly.
# They're more complex than unit tests but catch issues that unit tests might miss.
# ========================================================================================

class TestIntegrationScenarios:
    """Test realistic usage patterns and integration scenarios."""
    
    def test_complete_workflow_simulation(self, log_manager, mock_logger):
        """Test a complete workflow similar to the original test_logger.py.
        
        PYTEST: Integration test combines multiple operations to test real-world usage.
        This ensures individual components work together correctly.
        """
        # 1. Get pre-configured loggers
        logger_a = log_manager.get_logger("logger_a")
        logger_b = log_manager.get_logger("logger_b")
        
        # Verify initial state
        assert logger_a is not None
        assert logger_b is not None
        
        # 2. Test error case - getting nonexistent logger
        # This should raise an error since logger_c is not defined in the config file
        with pytest.raises(AssertionError, match="does not exist"):
            log_manager.get_logger("logger_c")
        
        # 3. Add new custom handler with fire emoji
        mock_logger.reset_mock()
        log_manager.add_handler("handler_console_fire", {
            "sink": "sys.stdout",
            "level": "info",
            "format": "🔥 <green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | {extra[logger_name]} | {file: <16} | <cyan>{function}:{line}</cyan> - <level>{message}</level>",
        })
        
        # 4. Add new logger using the fire handler
        log_manager.add_logger("logger_c", [{'handler': 'handler_console_fire', 'level': 'debug'}])
        
        # Now logger_c should be available
        logger_c = log_manager.get_logger("logger_c")
        assert logger_c is not None
        assert "handler_console_fire" in log_manager._handlers_map
        assert "logger_c" in log_manager._loggers_map
        
        # 5. Add another handler with format reference
        log_manager.add_handler("handler_console_fire2", {
            "sink": "sys.stdout",
            "level": "info",
            "format": "simple"  # References format from config
        })
        
        # 6. Update handler test
        mock_logger.reset_mock()
        log_manager.update_handler("handler_console_fire", {
            "sink": "sys.stdout",
            "level": "debug",
            "format": "🧯 <green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | {extra[logger_name]} | {file: <16} | <cyan>{function}:{line}</cyan> - <level>{message}</level>",
        })
        
        # Verify handler was updated (remove old, add new)
        mock_logger.remove.assert_called_once_with('123')  # Mock ID
        mock_logger.add.assert_called_once()
        
        # 7. Update logger with multiple handlers
        log_manager.update_logger("logger_c", [
            {'handler': 'handler_console_fire', 'level': 'ERROR'},
            {'handler': 'handler_console', 'level': 'error'}
        ])
        
        # Verify logger was updated with multiple handlers
        logger_c_config = log_manager._loggers_map["logger_c"]
        assert len(logger_c_config) == 2
        handler_names = [cfg['handler'] for cfg in logger_c_config]
        assert 'handler_console_fire' in handler_names
        assert 'handler_console' in handler_names
        
        # 8. Remove logger test
        log_manager.remove_logger("logger_c")
        assert "logger_c" not in log_manager._loggers_map
        
        # Logger should still exist but not be managed by LogManager
        # (This simulates the behavior where removed loggers can still log but aren't managed)
        
        # 9. Remove handler test
        mock_logger.reset_mock()
        log_manager.remove_handler("handler_console")
        
        # Verify handler was removed from logger and internal mapping
        mock_logger.remove.assert_called_once_with('123')  # Mock ID
        assert "handler_console" not in log_manager._handlers_map
        
        # Verify remaining loggers still work
        remaining_logger = log_manager.get_logger("logger_a")
        assert remaining_logger is not None
    
    def test_edge_cases_and_error_handling(self, log_manager):
        """Test edge cases and error handling scenarios from real usage.
        
        PYTEST: Integration test for error scenarios that might occur in production.
        """
        # Test adding logger with nonexistent handler reference
        # This should NOT be allowed - validation should prevent forward references
        with pytest.raises(KeyError, match="future_handler"):
            log_manager.add_logger('forward_ref_logger', [
                {'handler': 'future_handler', 'level': 'INFO'}
            ])
        
        # Test format reference vs custom format handling
        # Format reference (should look up in config)
        log_manager.add_handler('ref_format_handler', {
            'sink': 'sys.stdout',
            'format': 'simple',  # References config format
            'level': 'INFO'
        })
        
        # Custom format (should be used as-is)
        log_manager.add_handler('custom_format_handler', {
            'sink': 'sys.stdout', 
            'format': '{time} | CUSTOM | {message}',  # Custom format
            'level': 'INFO'
        })
        
        # Both handlers should be added successfully
        assert 'ref_format_handler' in log_manager._handlers_map
        assert 'custom_format_handler' in log_manager._handlers_map
