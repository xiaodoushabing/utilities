"""
Test suite for LoggingManager class.

This file contains all tests related to the LoggingManager component,
which handles Loguru configuration and logger management.
"""

import pytest
import os
import sys
import yaml
from pathlib import Path
from unittest.mock import patch, MagicMock, mock_open

from src.main.logging import LoggingManager

pytestmark = pytest.mark.unit


class TestLoggingManagerBasics:
    """Basic LoggingManager functionality tests."""
    
    def test_loggingmanager_can_be_created(self, logging_manager, default_config):
        """Test basic LoggingManager creation."""
        assert logging_manager is not None
        assert hasattr(logging_manager, '_handlers_map')
        assert hasattr(logging_manager, '_loggers_map')
        assert hasattr(logging_manager, '_config_path')

    def test_config_loading(self, logging_manager, default_config):
        """Test that configuration is loaded correctly."""
        config = logging_manager.config
        assert config is not None
        assert 'handlers' in config
        assert 'loggers' in config
        assert 'formats' in config

    def test_handlers_map_initialization(self, logging_manager):
        """Test that handlers map is properly initialized."""
        handlers_map = logging_manager._handlers_map
        assert isinstance(handlers_map, dict)
        # Should have handlers from default config
        assert len(handlers_map) > 0

    def test_loggers_map_initialization(self, logging_manager):
        """Test that loggers map is properly initialized."""
        loggers_map = logging_manager._loggers_map
        assert isinstance(loggers_map, dict)
        # Should have loggers from default config
        assert len(loggers_map) > 0


class TestLoggerManagement:
    """Test logger creation, updating, and removal."""
    
    def test_get_existing_logger(self, logging_manager):
        """Test retrieving an existing logger."""
        # logger_a should exist from default config
        logger = logging_manager.get_logger("logger_a")
        assert logger is not None

    def test_get_nonexistent_logger_raises_error(self, logging_manager):
        """Test that getting a non-existent logger raises AssertionError."""
        with pytest.raises(AssertionError, match="does not exist"):
            logging_manager.get_logger("nonexistent_logger")

    def test_add_new_logger(self, logging_manager):
        """Test adding a new logger."""
        new_handlers = [
            {'handler': 'handler_console', 'level': 'INFO'},
            {'handler': 'handler_file', 'level': 'DEBUG'}
        ]
        
        logging_manager.add_logger("new_logger", new_handlers)
        
        # Should be able to get the new logger
        logger = logging_manager.get_logger("new_logger")
        assert logger is not None

    def test_add_existing_logger_raises_error(self, logging_manager):
        """Test that adding an existing logger raises AssertionError."""
        new_handlers = [{'handler': 'handler_console', 'level': 'INFO'}]
        
        with pytest.raises(AssertionError, match="already exists"):
            logging_manager.add_logger("logger_a", new_handlers)

    def test_update_existing_logger(self, logging_manager):
        """Test updating an existing logger."""
        updated_handlers = [{'handler': 'handler_console', 'level': 'ERROR'}]
        
        logging_manager.update_logger("logger_a", updated_handlers)
        
        # Logger should still exist
        logger = logging_manager.get_logger("logger_a")
        assert logger is not None

    def test_update_nonexistent_logger_raises_error(self, logging_manager):
        """Test that updating a non-existent logger raises AssertionError."""
        handlers = [{'handler': 'handler_console', 'level': 'INFO'}]
        
        with pytest.raises(AssertionError, match="does not exist"):
            logging_manager.update_logger("nonexistent_logger", handlers)

    def test_remove_existing_logger(self, logging_manager):
        """Test removing an existing logger."""
        # First add a logger to remove
        logging_manager.add_logger("temp_logger", [{'handler': 'handler_console', 'level': 'INFO'}])
        
        # Remove it
        logging_manager.remove_logger("temp_logger")
        
        # Should no longer exist
        with pytest.raises(AssertionError):
            logging_manager.get_logger("temp_logger")

    def test_remove_nonexistent_logger_raises_error(self, logging_manager):
        """Test that removing a non-existent logger raises AssertionError."""
        with pytest.raises(AssertionError, match="does not exist"):
            logging_manager.remove_logger("nonexistent_logger")


class TestHandlerManagement:
    """Test handler creation, updating, and removal."""
    
    def test_add_new_handler(self, logging_manager):
        """Test adding a new handler."""
        handler_config = {
            'sink': 'test.log',
            'format': 'simple',
            'level': 'DEBUG'
        }
        
        logging_manager.add_handler("new_handler", handler_config)
        
        # Handler should be in the handlers map
        assert "new_handler" in logging_manager._handlers_map

    def test_add_existing_handler_raises_error(self, logging_manager):
        """Test that adding an existing handler raises AssertionError."""
        handler_config = {'sink': 'test.log', 'format': 'simple', 'level': 'DEBUG'}
        
        with pytest.raises(AssertionError, match="already exists"):
            logging_manager.add_handler("handler_file", handler_config)

    def test_update_existing_handler(self, logging_manager):
        """Test updating an existing handler."""
        updated_config = {
            'sink': 'updated.log',
            'format': 'simple',
            'level': 'INFO'
        }
        
        logging_manager.update_handler("handler_file", updated_config)
        
        # Handler should still exist
        assert "handler_file" in logging_manager._handlers_map

    def test_update_nonexistent_handler_raises_error(self, logging_manager):
        """Test that updating a non-existent handler raises AssertionError."""
        handler_config = {'sink': 'test.log', 'format': 'simple', 'level': 'DEBUG'}
        
        with pytest.raises(AssertionError, match="does not exist"):
            logging_manager.update_handler("nonexistent_handler", handler_config)

    def test_remove_existing_handler(self, logging_manager):
        """Test removing an existing handler."""
        # First add a handler to remove
        logging_manager.add_handler("temp_handler", {
            'sink': 'temp.log',
            'format': 'simple',
            'level': 'DEBUG'
        })
        
        # Remove it
        logging_manager.remove_handler("temp_handler")
        
        # Should no longer exist
        assert "temp_handler" not in logging_manager._handlers_map

    def test_remove_nonexistent_handler_raises_error(self, logging_manager):
        """Test that removing a non-existent handler raises AssertionError."""
        with pytest.raises(AssertionError, match="does not exist"):
            logging_manager.remove_handler("nonexistent_handler")


class TestConfigurationHandling:
    """Test configuration file handling and validation."""
    
    def test_config_property_returns_dict(self, logging_manager):
        """Test that config property returns a dictionary."""
        config = logging_manager.config
        assert isinstance(config, dict)

    def test_config_path_property(self, logging_manager):
        """Test that config path property returns a valid path."""
        config_path = logging_manager._config_path
        assert config_path is not None
        # Could be string or Path object depending on implementation
        assert isinstance(config_path, (str, Path))

    def test_invalid_config_handling(self, mock_logger):
        """Test handling of invalid configuration files."""
        invalid_yaml = "invalid: yaml: content: ["
        
        with patch('builtins.open', mock_open(read_data=invalid_yaml)):
            with pytest.raises(yaml.YAMLError):
                LoggingManager()

    def test_missing_config_file_handling(self, mock_logger):
        """Test handling when config file doesn't exist."""
        with patch('builtins.open', side_effect=FileNotFoundError("File not found")):
            with pytest.raises(FileNotFoundError):
                LoggingManager("/nonexistent/config.yaml")


class TestCleanup:
    """Test cleanup functionality."""
    
    def test_cleanup_method_exists(self, logging_manager):
        """Test that cleanup method exists and is callable."""
        assert hasattr(logging_manager, 'cleanup')
        assert callable(logging_manager.cleanup)

    def test_cleanup_can_be_called(self, logging_manager):
        """Test that cleanup can be called without errors."""
        # Should not raise any exceptions
        logging_manager.cleanup()


@pytest.mark.parametrize("config_variant", [
    {
        'formats': {'custom': '{time} - {level} - {message}'},
        'handlers': {
            'custom_handler': {
                'sink': 'custom.log',
                'format': 'custom',
                'level': 'INFO'
            }
        },
        'loggers': {
            'custom_logger': [
                {'handler': 'custom_handler', 'level': 'INFO'}
            ]
        }
    },
    {
        'formats': {'simple': '{message}'},
        'handlers': {
            'console_only': {
                'sink': 'sys.stdout',
                'format': 'simple',
                'level': 'DEBUG'
            }
        },
        'loggers': {
            'debug_logger': [
                {'handler': 'console_only', 'level': 'DEBUG'}
            ]
        }
    }
])
def test_different_config_variants(mock_logger, config_variant):
    """Test LoggingManager with different configuration variants."""
    with patch('builtins.open', mock_open(read_data=yaml.dump(config_variant))):
        manager = LoggingManager()
        
        # Should load the configuration successfully - check structure instead of exact match
        # as LoggingManager processes the config (expands format refs, adds filters, etc.)
        config = manager.config
        
        # Check that the main sections exist
        assert 'formats' in config
        assert 'handlers' in config
        assert 'loggers' in config
        
        # Check that formats from original config are present
        for format_name in config_variant['formats']:
            assert format_name in config['formats']
            
        # Check that handlers from original config are present
        for handler_name in config_variant['handlers']:
            assert handler_name in config['handlers']
            
        # Check that loggers from original config are present  
        for logger_name in config_variant['loggers']:
            assert logger_name in config['loggers']
        
        # Should be able to get loggers defined in config
        logger_names = list(config_variant['loggers'].keys())
        for logger_name in logger_names:
            logger = manager.get_logger(logger_name)
            assert logger is not None
        
        # Cleanup
        manager.cleanup()
