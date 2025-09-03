"""
Test the new component-based architecture after refactoring.

This demonstrates that:
1. The refactored LogManager maintains backward compatibility
2. Individual components can be imported and used independently
3. The composition pattern provides better separation of concerns
"""
import pytest
from utilities.logger import LogManager, LoggingManager, CopyManager, DistributedCoordinator


def test_component_imports():
    """Test that all component classes can be imported individually."""
    # All classes should be importable
    assert LogManager is not None
    assert LoggingManager is not None
    assert CopyManager is not None
    assert DistributedCoordinator is not None


def test_individual_component_creation():
    """Test that components can be created independently."""
    # Test DistributedCoordinator
    coordinator = DistributedCoordinator()
    assert coordinator is not None
    assert isinstance(coordinator.copy_enabled, bool)
    # Test CopyManager
    copy_manager = CopyManager(enabled=False)  # Disable to avoid side effects
    assert copy_manager is not None
    assert copy_manager.is_enabled() == False
    
    # Test LoggingManager
    logging_manager = LoggingManager()
    assert logging_manager is not None
    assert hasattr(logging_manager, '_handlers_map')
    assert hasattr(logging_manager, '_loggers_map')
    
    # Cleanup
    logging_manager.cleanup()
    copy_manager.cleanup()


def test_logmanager_composition():
    """Test that LogManager properly composes the components."""
    log_manager = LogManager()
    
    # Verify all components are present
    assert hasattr(log_manager, '_coordinator')
    assert hasattr(log_manager, '_logging_manager') 
    assert hasattr(log_manager, '_copy_manager')
    
    # Verify components are the correct types
    assert isinstance(log_manager._coordinator, DistributedCoordinator)
    assert isinstance(log_manager._logging_manager, LoggingManager)
    assert isinstance(log_manager._copy_manager, CopyManager)
    
    # Test backward compatibility properties
    assert hasattr(log_manager, '_handlers_map')
    assert hasattr(log_manager, '_loggers_map')
    assert hasattr(log_manager, '_config_path')
    assert hasattr(log_manager, 'config')
    assert hasattr(log_manager, 'copy_enabled')


def test_component_delegation():
    """Test that LogManager properly delegates to components."""
    log_manager = LogManager()
    
    # Test logging delegation
    logger = log_manager.get_logger('default_task')
    assert logger is not None
    
    # Test copy delegation
    status = log_manager.get_copy_status()
    assert 'copy_enabled' in status
    assert isinstance(status['copy_enabled'], bool)
    # Test that operations list is empty initially
    operations = log_manager.list_copy_operations()
    assert operations == []


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
