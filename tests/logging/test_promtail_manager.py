
"""
Test suite for PromtailManager class.

Tests the PromtailManager's functionality including:
- Initialization with various configurations
- Starting and stopping Promtail agent
- Configuration validation
- Error handling
- Cleanup operations
"""

import pytest
from unittest.mock import patch, call

from src.main.logging import PromtailManager


class TestPromtailManagerInitialization:
    """Test PromtailManager initialization with different configurations."""

    def test_init_with_empty_config(self):
        """Test initialization with empty configuration."""
        manager = PromtailManager()
        
        assert manager.instance_name == ""
        assert manager.target_paths == []
        assert manager.log_level == ""
        assert manager.static_labels == {}

    def test_init_with_full_config(self, promtail_config):
        """Test initialization with complete configuration."""
        manager = PromtailManager(promtail_config)
        
        assert manager.instance_name == "test-instance"
        assert manager.target_paths == ["/var/log/*.log", "/app/logs/*.txt"]
        assert manager.log_level == "INFO"
        assert manager.static_labels == {
            "run_id": "test-run",
            "script_id": "test-script"
        }

    @pytest.mark.parametrize("config,expected", [
        ({"instance_name": "test"}, "test"),
        ({"instance_name": ""}, ""),
        ({}, ""),
    ])
    def test_init_instance_name_variations(self, config, expected):
        """Test instance_name initialization with various values."""
        manager = PromtailManager(config)
        assert manager.instance_name == expected

    @pytest.mark.parametrize("config,expected", [
        ({"target_paths": ["/test/*.log"]}, ["/test/*.log"]),
        ({"target_paths": []}, []),
        ({}, []),
    ])
    def test_init_target_paths_variations(self, config, expected):
        """Test target_paths initialization with various values."""
        manager = PromtailManager(config)
        assert manager.target_paths == expected

    @pytest.mark.parametrize("config,expected", [
        ({"log_level": "debug"}, "DEBUG"),
        ({"log_level": "INFO"}, "INFO"),
        ({"log_level": "Error"}, "ERROR"),
        ({"log_level": ""}, ""),
        ({}, ""),
    ])
    def test_init_log_level_case_handling(self, config, expected):
        """Test log_level initialization with case conversion."""
        manager = PromtailManager(config)
        assert manager.log_level == expected


class TestPromtailManagerStartPromtail:
    """Test PromtailManager start_promtail functionality."""

    def test_start_promtail_with_manager_config(self, mock_promtail_agent, promtail_config):
        """Test starting Promtail with configuration from manager initialization."""
        manager = PromtailManager(promtail_config)
        manager.start_promtail()
        
        # Verify PromtailAgent was called with correct parameters
        mock_promtail_agent['class'].assert_called_once_with(
            instance_name="test-instance",
            target_paths=["/var/log/*.log", "/app/logs/*.txt"],
            log_level="INFO"
        )
        
        # Verify agent methods were called
        mock_promtail_agent['instance'].start.assert_called_once()
        
        # Verify static labels were set
        expected_labels = {
            "run_id": "test-run",
            "script_id": "test-script"
        }
        pipeline_stage = mock_promtail_agent['config'].scrape_configs[0]["pipeline_stages"][0]
        for key, value in expected_labels.items():
            assert pipeline_stage["static_labels"][key] == value

    def test_start_promtail_with_runtime_config(self, mock_promtail_agent, minimal_promtail_config):
        """Test starting Promtail with configuration provided at runtime."""
        manager = PromtailManager()
        runtime_config = {
            "instance_name": "runtime-test",
            "target_paths": ["/runtime/*.log"],
            "log_level": "DEBUG",
            "static_labels": {"env": "runtime"}
        }
        
        manager.start_promtail(runtime_config)
        
        # Verify PromtailAgent was called with runtime config
        mock_promtail_agent['class'].assert_called_once_with(
            instance_name="runtime-test",
            target_paths=["/runtime/*.log"],
            log_level="DEBUG"
        )
        
        # Verify runtime config updated manager state
        assert manager.instance_name == "runtime-test"
        assert manager.target_paths == ["/runtime/*.log"]
        assert manager.log_level == "DEBUG"
        assert manager.static_labels == {"env": "runtime"}

    def test_start_promtail_runtime_config_overrides_init_config(self, mock_promtail_agent, promtail_config):
        """Test that runtime config overrides initialization config."""
        manager = PromtailManager(promtail_config)
        runtime_config = {
            "instance_name": "override-test",
            "target_paths": ["/override/*.log"]
        }
        
        manager.start_promtail(runtime_config)
        
        # Verify PromtailAgent was called with overridden values
        mock_promtail_agent['class'].assert_called_once_with(
            instance_name="override-test",
            target_paths=["/override/*.log"],
            log_level="INFO"  # This should remain from init config
        )

    @pytest.mark.parametrize("missing_field,config", [
        ("instance_name", {"target_paths": ["/test/*.log"]}),
        ("target_paths", {"instance_name": "test"}),
    ])
    def test_start_promtail_missing_required_fields(self, mock_promtail_agent, missing_field, config):
        """Test that ValueError is raised when required fields are missing."""
        manager = PromtailManager()
        
        with pytest.raises(ValueError) as exc_info:
            manager.start_promtail(config)
        
        if missing_field == "instance_name":
            assert "instance name is required" in str(exc_info.value)
        elif missing_field == "target_paths":
            assert "target paths are required" in str(exc_info.value)

    def test_start_promtail_empty_values_filtered(self, mock_promtail_agent):
        """Test that empty values are filtered out when calling PromtailAgent."""
        manager = PromtailManager()
        config = {
            "instance_name": "test",
            "target_paths": ["/test/*.log"],
            "log_level": ""  # Empty value should be filtered
        }
        
        manager.start_promtail(config)
        
        # Verify PromtailAgent was called without empty log_level
        mock_promtail_agent['class'].assert_called_once_with(
            instance_name="test",
            target_paths=["/test/*.log"]
        )

    def test_start_promtail_static_labels_applied(self, mock_promtail_agent):
        """Test that static labels are properly applied to promtail config."""
        manager = PromtailManager()
        config = {
            "instance_name": "test",
            "target_paths": ["/test/*.log"],
            "static_labels": {
                "key1": "value1",
                "key2": "",      # Empty value should be skipped
                "key3": None,    # None value should be skipped
                "key4": "value4"
            }
        }
        
        manager.start_promtail(config)
        
        # Verify only non-empty labels were set
        pipeline_stage = mock_promtail_agent['config'].scrape_configs[0]["pipeline_stages"][0]
        assert pipeline_stage["static_labels"]["key1"] == "value1"
        assert pipeline_stage["static_labels"]["key4"] == "value4"
        assert "key2" not in pipeline_stage["static_labels"]
        assert "key3" not in pipeline_stage["static_labels"]


class TestPromtailManagerStopPromtail:
    """Test PromtailManager stop_promtail functionality."""

    def test_stop_promtail_success(self, mock_promtail_agent, minimal_promtail_config):
        """Test successful stopping of Promtail agent."""
        manager = PromtailManager()
        manager.start_promtail(minimal_promtail_config)
        
        with patch('builtins.print') as mock_print:
            manager.stop_promtail()
        
        # Verify agent.stop() was called
        mock_promtail_agent['instance'].stop.assert_called_once()
        
        # Verify agent was set to None
        assert manager.promtail_agent is None
        
        # Verify success message was printed
        mock_print.assert_called_with("PromtailAgent stopped")

    def test_stop_promtail_with_exception(self, mock_promtail_agent, minimal_promtail_config):
        """Test stopping Promtail agent when exception occurs."""
        manager = PromtailManager()
        manager.start_promtail(minimal_promtail_config)
        
        # Configure mock to raise exception
        mock_promtail_agent['instance'].stop.side_effect = Exception("Stop failed")
        
        with patch('builtins.print') as mock_print:
            manager.stop_promtail()
        
        # Verify warning message was printed
        mock_print.assert_called_with("WARNING: Could not stop PromtailAgent: Stop failed")

    def test_stop_promtail_without_starting(self):
        """Test stopping Promtail agent when it was never started."""
        manager = PromtailManager()
        
        # Should not raise, but should print a warning
        with patch('builtins.print') as mock_print:
            manager.stop_promtail()
            printed_args = mock_print.call_args[0][0]
            assert "WARNING" in printed_args
            assert "PromtailAgent" in printed_args


class TestPromtailManagerCleanup:
    """Test PromtailManager cleanup functionality."""

    def test_cleanup_with_active_agent(self, mock_promtail_agent, minimal_promtail_config):
        """Test cleanup when Promtail agent is active."""
        manager = PromtailManager()
        manager.start_promtail(minimal_promtail_config)
        
        with patch('builtins.print') as mock_print:
            manager.cleanup()
        
        # Verify stop was called
        mock_promtail_agent['instance'].stop.assert_called_once()
        
        # Verify cleanup messages were printed
        expected_calls = [
            call("PromtailManager cleanup initiated..."),
            call("PromtailAgent stopped"),
            call("PromtailManager cleanup completed.")
        ]
        mock_print.assert_has_calls(expected_calls)

    def test_cleanup_without_active_agent(self):
        """Test cleanup when no Promtail agent is active."""
        manager = PromtailManager()
        manager.promtail_agent = None
        
        with patch('builtins.print') as mock_print:
            manager.cleanup()
        
        # Should only print cleanup completion message
        mock_print.assert_called_once_with("No PromtailAgent to clean up.")

    def test_cleanup_with_exception_in_stop(self, mock_promtail_agent, minimal_promtail_config):
        """Test cleanup when stop_promtail raises an exception."""
        manager = PromtailManager()
        manager.start_promtail(minimal_promtail_config)
        
        # Configure mock to raise exception
        mock_promtail_agent['instance'].stop.side_effect = Exception("Stop failed")
        
        with patch('builtins.print') as mock_print:
            manager.cleanup()
        
        # Verify both error and completion messages were printed
        expected_calls = [
            call("PromtailManager cleanup initiated..."),
            call("WARNING: Could not stop PromtailAgent: Stop failed"),
            call("PromtailManager cleanup completed.")
        ]
        mock_print.assert_has_calls(expected_calls)


class TestPromtailManagerIntegration:
    """Integration tests for PromtailManager."""

    def test_full_lifecycle(self, mock_promtail_agent, promtail_config):
        """Test complete lifecycle: init -> start -> stop -> cleanup."""
        manager = PromtailManager(promtail_config)
        
        # Start
        manager.start_promtail()
        assert hasattr(manager, 'promtail_agent')
        mock_promtail_agent['instance'].start.assert_called_once()
        
        # Stop
        manager.stop_promtail()
        assert manager.promtail_agent is None
        mock_promtail_agent['instance'].stop.assert_called_once()
        
        # Cleanup (should handle gracefully when agent is None)
        with patch('builtins.print'):
            manager.cleanup()

    def test_multiple_start_calls(self, mock_promtail_agent, minimal_promtail_config):
        """Test calling start_promtail multiple times."""
        manager = PromtailManager()
        
        # First start
        manager.start_promtail(minimal_promtail_config)
        first_agent = manager.promtail_agent
        
        # Second start (should create new agent)
        new_config = {
            "instance_name": "new-test",
            "target_paths": ["/new/*.log"]
        }
        manager.start_promtail(new_config)
        second_agent = manager.promtail_agent
        
        # Verify two separate calls to PromtailAgent
        assert mock_promtail_agent['class'].call_count == 2
        
        # Verify configuration was updated
        assert manager.instance_name == "new-test"
        assert manager.target_paths == ["/new/*.log"]