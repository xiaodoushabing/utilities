"""
Test suite for LogManager class (main coordinator).

This file contains all tests related to the main LogManager class,
which coordinates between LoggingManager, CopyManager, and DistributedCoordinator.
"""

import pytest
import atexit
from unittest.mock import patch, MagicMock, call

from src.main.logger import LogManager

pytestmark = pytest.mark.unit


class TestLogManagerInitialization:
    """Test LogManager initialization and composition."""
    
    @patch('atexit.register')
    def test_logmanager_can_be_created(self, mock_atexit, mock_logger, default_config):
        """Test basic LogManager creation."""
        with patch('builtins.open', mock_open(read_data=yaml.dump(default_config))):
            manager = LogManager()
            
            # Should have all component managers
            assert hasattr(manager, '_coordinator')
            assert hasattr(manager, '_logging_manager')
            assert hasattr(manager, '_copy_manager')
            
            # Should register cleanup
            mock_atexit.assert_called_once_with(manager._cleanup)

    @patch('atexit.register')
    @patch('os.path.exists')
    @patch('os.path.isfile')
    def test_initialization_with_custom_config(self, mock_isfile, mock_exists, mock_atexit, mock_logger, default_config):
        """Test LogManager initialization with custom config path."""
        custom_config_path = "/custom/config.yaml"
        
        # Mock the file validation to make it think the custom config exists
        mock_exists.return_value = True
        mock_isfile.return_value = True
        
        with patch('builtins.open', mock_open(read_data=yaml.dump(default_config))):
            manager = LogManager(config_path=custom_config_path)
            
            # Should pass config path to logging manager
            assert str(manager._logging_manager._config_path) == custom_config_path

    @patch('atexit.register')
    def test_initialization_with_custom_timezone(self, mock_atexit, mock_logger, default_config):
        """Test LogManager initialization with custom timezone."""
        custom_timezone = "UTC"
        
        with patch('builtins.open', mock_open(read_data=yaml.dump(default_config))):
            manager = LogManager(timezone=custom_timezone)
            
            # Should initialize with custom timezone
            assert manager is not None

    def test_component_composition(self, log_manager):
        """Test that LogManager properly composes all components."""
        # Should have all three component managers
        assert log_manager._coordinator is not None
        assert log_manager._logging_manager is not None
        assert log_manager._copy_manager is not None
        
        # Copy manager should be configured based on coordinator
        coordinator_enabled = log_manager._coordinator.copy_enabled
        copy_manager_enabled = log_manager._copy_manager._enabled
        assert coordinator_enabled == copy_manager_enabled


class TestLoggingDelegation:
    """Test that LogManager properly delegates logging methods to LoggingManager."""
    
    def test_config_property_delegation(self, log_manager):
        """Test that config property is delegated to LoggingManager."""
        config = log_manager.config
        assert config is not None
        assert config == log_manager._logging_manager.config

    def test_config_path_property_delegation(self, log_manager):
        """Test that _config_path property is delegated to LoggingManager."""
        config_path = log_manager._config_path
        assert config_path is not None
        assert config_path == log_manager._logging_manager._config_path

    def test_handlers_map_property_delegation(self, log_manager):
        """Test that _handlers_map property is delegated to LoggingManager."""
        handlers_map = log_manager._handlers_map
        assert handlers_map is not None
        assert handlers_map == log_manager._logging_manager._handlers_map

    def test_loggers_map_property_delegation(self, log_manager):
        """Test that _loggers_map property is delegated to LoggingManager."""
        loggers_map = log_manager._loggers_map
        assert loggers_map is not None
        assert loggers_map == log_manager._logging_manager._loggers_map

    def test_get_logger_delegation(self, log_manager):
        """Test that get_logger is delegated to LoggingManager."""
        # Should delegate to logging manager
        with patch.object(log_manager._logging_manager, 'get_logger') as mock_get:
            mock_get.return_value = MagicMock()
            
            result = log_manager.get_logger("test_logger")
            
            mock_get.assert_called_once_with("test_logger")
            assert result == mock_get.return_value

    def test_add_logger_delegation(self, log_manager):
        """Test that add_logger is delegated to LoggingManager."""
        handlers = [('handler1', {'level': 'INFO'})]
        
        with patch.object(log_manager._logging_manager, 'add_logger') as mock_add:
            log_manager.add_logger("new_logger", handlers)
            mock_add.assert_called_once_with("new_logger", handlers)

    def test_update_logger_delegation(self, log_manager):
        """Test that update_logger is delegated to LoggingManager."""
        handlers = [('handler1', {'level': 'DEBUG'})]
        
        with patch.object(log_manager._logging_manager, 'update_logger') as mock_update:
            log_manager.update_logger("existing_logger", handlers)
            mock_update.assert_called_once_with("existing_logger", handlers)

    def test_remove_logger_delegation(self, log_manager):
        """Test that remove_logger is delegated to LoggingManager."""
        with patch.object(log_manager._logging_manager, 'remove_logger') as mock_remove:
            log_manager.remove_logger("test_logger")
            mock_remove.assert_called_once_with("test_logger")

    def test_add_handler_delegation(self, log_manager):
        """Test that add_handler is delegated to LoggingManager."""
        handler_conf = {'sink': 'test.log', 'level': 'INFO'}
        
        with patch.object(log_manager._logging_manager, 'add_handler') as mock_add:
            log_manager.add_handler("new_handler", handler_conf)
            mock_add.assert_called_once_with("new_handler", handler_conf)

    def test_update_handler_delegation(self, log_manager):
        """Test that update_handler is delegated to LoggingManager."""
        handler_conf = {'sink': 'updated.log', 'level': 'DEBUG'}
        
        with patch.object(log_manager._logging_manager, 'update_handler') as mock_update:
            log_manager.update_handler("existing_handler", handler_conf)
            mock_update.assert_called_once_with("existing_handler", handler_conf)

    def test_remove_handler_delegation(self, log_manager):
        """Test that remove_handler is delegated to LoggingManager."""
        with patch.object(log_manager._logging_manager, 'remove_handler') as mock_remove:
            log_manager.remove_handler("test_handler")
            mock_remove.assert_called_once_with("test_handler")


class TestDistributedCoordinationDelegation:
    """Test that LogManager properly delegates coordination methods to DistributedCoordinator."""
    
    def test_copy_enabled_property_delegation(self, log_manager):
        """Test that copy_enabled property is delegated to DistributedCoordinator."""
        copy_enabled = log_manager.copy_enabled
        assert copy_enabled == log_manager._coordinator.copy_enabled

    def test_get_copy_status_delegation(self, log_manager):
        """Test that get_copy_status is delegated to DistributedCoordinator."""
        with patch.object(log_manager._coordinator, 'get_copy_status') as mock_status:
            mock_status.return_value = {'enabled': True, 'reason': 'test'}
            
            result = log_manager.get_copy_status()
            
            mock_status.assert_called_once()
            assert result == mock_status.return_value


class TestCopyDelegation:
    """Test that LogManager properly delegates copy methods to CopyManager."""
    
    def test_start_copy_from_config_delegation(self, log_manager):
        """Test that start_copy_from_config is delegated to CopyManager."""
        config = {'test': 'config'}
        
        with patch.object(log_manager._copy_manager, 'start_copy_from_config') as mock_start:
            log_manager.start_copy_from_config(config)
            mock_start.assert_called_once_with(config)

    def test_start_copy_delegation(self, log_manager):
        """Test that start_copy is delegated to CopyManager."""
        kwargs = {
            'copy_name': 'test',
            'path_patterns': ['/tmp/*.log'],
            'copy_destination': 'hdfs://dest/'
        }
        
        with patch.object(log_manager._copy_manager, 'start_copy') as mock_start:
            log_manager.start_copy(**kwargs)
            mock_start.assert_called_once_with(**kwargs)

    def test_start_copy_filters_none_values(self, log_manager):
        """Test that start_copy filters out None values before delegation."""
        kwargs = {
            'copy_name': 'test',
            'path_patterns': ['/tmp/*.log'],
            'copy_destination': 'hdfs://dest/',
            'root_dir': None,  # Should be filtered out
            'copy_interval': None  # Should be filtered out
        }
        
        expected_kwargs = {
            'copy_name': 'test',
            'path_patterns': ['/tmp/*.log'],
            'copy_destination': 'hdfs://dest/'
        }
        
        with patch.object(log_manager._copy_manager, 'start_copy') as mock_start:
            log_manager.start_copy(**kwargs)
            mock_start.assert_called_once_with(**expected_kwargs)

    def test_stop_copy_delegation(self, log_manager):
        """Test that stop_copy is delegated to CopyManager."""
        with patch.object(log_manager._copy_manager, 'stop_copy') as mock_stop:
            mock_stop.return_value = True
            
            result = log_manager.stop_copy('test_copy')
            
            mock_stop.assert_called_once_with(copy_name='test_copy')
            assert result is True

    def test_stop_copy_filters_none_values(self, log_manager):
        """Test that stop_copy filters out None values before delegation."""
        with patch.object(log_manager._copy_manager, 'stop_copy') as mock_stop:
            mock_stop.return_value = True
            
            result = log_manager.stop_copy('test_copy', timeout=None)
            
            mock_stop.assert_called_once_with(copy_name='test_copy')
            assert result is True

    def test_stop_all_copy_delegation(self, log_manager):
        """Test that stop_all_copy is delegated to CopyManager."""
        with patch.object(log_manager._copy_manager, 'stop_all_copy_operations') as mock_stop_all:
            mock_stop_all.return_value = []
            
            result = log_manager.stop_all_copy()
            
            mock_stop_all.assert_called_once()
            assert result == []

    def test_list_copy_operations_delegation(self, log_manager):
        """Test that list_copy_operations is delegated to CopyManager."""
        with patch.object(log_manager._copy_manager, 'list_copy_operations') as mock_list:
            mock_list.return_value = [{'copy_name': 'test'}]
            
            result = log_manager.list_copy_operations()
            
            mock_list.assert_called_once()
            assert result == mock_list.return_value

    def test_trigger_copy_now_delegation(self, log_manager):
        """Test that trigger_copy_now is delegated to CopyManager."""
        with patch.object(log_manager._copy_manager, 'trigger_copy_now') as mock_trigger:
            log_manager.trigger_copy_now('test_copy')
            mock_trigger.assert_called_once_with('test_copy')

    def test_trigger_copy_now_all_operations(self, log_manager):
        """Test that trigger_copy_now without arguments delegates properly."""
        with patch.object(log_manager._copy_manager, 'trigger_copy_now') as mock_trigger:
            log_manager.trigger_copy_now()
            mock_trigger.assert_called_once_with(None)


class TestBackwardCompatibility:
    """Test that the refactored LogManager maintains backward compatibility."""
    
    def test_maintains_original_interface(self, log_manager):
        """Test that LogManager maintains its original public interface."""
        # All original methods should still exist
        original_methods = [
            'get_logger', 'add_logger', 'update_logger', 'remove_logger',
            'add_handler', 'update_handler', 'remove_handler',
            'start_copy', 'stop_copy', 'start_copy_from_config',
            'list_copy_operations', 'trigger_copy_now'
        ]
        
        for method_name in original_methods:
            assert hasattr(log_manager, method_name)
            assert callable(getattr(log_manager, method_name))

    def test_original_properties_exist(self, log_manager):
        """Test that original properties still exist."""
        original_properties = [
            'config', '_config_path', '_handlers_map', '_loggers_map', 'copy_enabled'
        ]
        
        for prop_name in original_properties:
            assert hasattr(log_manager, prop_name)

    def test_can_use_as_context_manager(self, mock_logger, default_config):
        """Test that LogManager can still be used in patterns that expect cleanup."""
        with patch('atexit.register'):
            with patch('builtins.open', mock_open(read_data=yaml.dump(default_config))):
                manager = LogManager()
                
                # Should be able to call cleanup manually
                manager._cleanup()


class TestCleanup:
    """Test cleanup functionality."""
    
    def test_cleanup_calls_all_components(self, log_manager):
        """Test that cleanup calls cleanup on all components."""
        with patch.object(log_manager._logging_manager, 'cleanup') as mock_logging_cleanup:
            with patch.object(log_manager._copy_manager, 'cleanup') as mock_copy_cleanup:
                log_manager._cleanup()
                
                mock_logging_cleanup.assert_called_once()
                mock_copy_cleanup.assert_called_once()

    def test_cleanup_can_be_called_multiple_times(self, log_manager):
        """Test that cleanup can be called multiple times safely."""
        # Should not raise any exceptions
        log_manager._cleanup()
        log_manager._cleanup()  # Second call should be safe

    def test_cleanup_with_timeout(self, log_manager):
        """Test cleanup with timeout parameter."""
        with patch.object(log_manager._copy_manager, 'cleanup') as mock_copy_cleanup:
            log_manager._cleanup(timeout=30.0)
            # Copy manager should receive the timeout
            mock_copy_cleanup.assert_called_once_with(timeout=30.0)


class TestIntegration:
    """Integration tests to verify components work together correctly."""
    
    def test_copy_manager_enabled_state_matches_coordinator(self, log_manager):
        """Test that CopyManager enabled state matches DistributedCoordinator."""
        coordinator_enabled = log_manager._coordinator.copy_enabled
        copy_manager_enabled = log_manager._copy_manager._enabled
        
        assert coordinator_enabled == copy_manager_enabled

    def test_copy_operations_respect_coordinator_state(self, log_manager, copy_defaults):
        """Test that copy operations respect the coordinator's enabled state."""
        if not log_manager._coordinator.copy_enabled:
            # If coordinator says copy is disabled, copy operations should fail
            with pytest.raises((RuntimeError, ValueError)):
                log_manager.start_copy(**copy_defaults)
        else:
            # If coordinator says copy is enabled, operations should work
            with patch.object(log_manager._copy_manager, 'start_copy') as mock_start:
                log_manager.start_copy(**copy_defaults)
                mock_start.assert_called_once()

    def test_config_shared_between_components(self, log_manager):
        """Test that configuration is properly shared between components."""
        # LogManager should get its config from LoggingManager
        assert log_manager.config == log_manager._logging_manager.config
        
        # CopyManager should get copy-specific config
        copy_config = log_manager.config.get("copy_manager", {})
        assert log_manager._copy_manager.config == copy_config


# Import required for mock_open
import yaml
from unittest.mock import mock_open
