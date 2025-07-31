# ========================================================================================
# PYTEST LEARNING TIP 💡
# Integration tests verify that multiple components work together correctly.
# They're more complex than unit tests but catch issues that unit tests might miss.
# ========================================================================================

import pytest
pytestmark = pytest.mark.integration

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
    
    def test_bidirectional_mapping_consistency(self, log_manager):
        """Test that bidirectional mapping stays consistent during complex operations.
        
        PYTEST: Integration test for complex many-to-many handler-logger relationships.
        This ensures the bidirectional mapping system works correctly in realistic scenarios.
        """
        # Create complex mapping scenario
        log_manager.add_handler("handler_A", {'sink': 'sys.stdout', 'format': 'simple', 'level': 'INFO'})
        log_manager.add_handler("handler_B", {'sink': 'sys.stderr', 'format': 'simple', 'level': 'DEBUG'})
        
        # Logger with multiple handlers
        log_manager.add_logger("multi_logger", [
            {'handler': 'handler_A', 'level': 'INFO'},
            {'handler': 'handler_B', 'level': 'DEBUG'}
        ])
        
        # Additional logger for handler_A
        log_manager.add_logger("single_logger", [
            {'handler': 'handler_A', 'level': 'ERROR'}
        ])
        
        # Verify complex bidirectional mapping
        # multi_logger should reference both handlers
        multi_handlers = [h["handler"] for h in log_manager._loggers_map["multi_logger"]]
        assert "handler_A" in multi_handlers
        assert "handler_B" in multi_handlers
        
        # handler_A should reference both loggers
        assert "multi_logger" in log_manager._handlers_map["handler_A"]["loggers"]
        assert "single_logger" in log_manager._handlers_map["handler_A"]["loggers"]
        
        # handler_B should only reference multi_logger
        assert "multi_logger" in log_manager._handlers_map["handler_B"]["loggers"]
        assert "single_logger" not in log_manager._handlers_map["handler_B"]["loggers"]
        
        # Remove handler_A and verify cleanup
        log_manager.remove_handler("handler_A")
        
        # multi_logger should only reference handler_B now
        multi_handlers_after = [h["handler"] for h in log_manager._loggers_map["multi_logger"]]
        assert "handler_A" not in multi_handlers_after
        assert "handler_B" in multi_handlers_after
        assert len(multi_handlers_after) == 1
        
        # single_logger should still exist but have no handlers
        assert "single_logger" in log_manager._loggers_map  # Logger still exists
        assert len(log_manager._loggers_map["single_logger"]) == 0  # But no handlers
        
        # handler_B should still only reference multi_logger
        assert "multi_logger" in log_manager._handlers_map["handler_B"]["loggers"]
        assert len(log_manager._handlers_map["handler_B"]["loggers"]) == 1
    
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
