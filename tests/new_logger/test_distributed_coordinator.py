"""
Test suite for DistributedCoordinator class.

This file contains all tests related to the DistributedCoordinator component,
which handles distributed system coordination and environment detection.
"""

import pytest
import os
from unittest.mock import patch, MagicMock

from src.main.logger import DistributedCoordinator

pytestmark = pytest.mark.unit


class TestDistributedCoordinatorBasics:
    """Basic DistributedCoordinator functionality tests."""
    
    def test_coordinator_can_be_created(self, distributed_coordinator):
        """Test basic DistributedCoordinator creation."""
        assert distributed_coordinator is not None
        assert hasattr(distributed_coordinator, 'copy_enabled')

    def test_copy_enabled_property_returns_boolean(self, distributed_coordinator):
        """Test that copy_enabled property returns a boolean."""
        enabled = distributed_coordinator.copy_enabled
        assert isinstance(enabled, bool)

    def test_get_copy_status_returns_dict(self, distributed_coordinator):
        """Test that get_copy_status returns a dictionary."""
        status = distributed_coordinator.get_copy_status()
        assert isinstance(status, dict)
        assert 'copy_enabled' in status
        assert 'reason' in status


class TestEnvironmentDetection:
    """Test environment detection and copy enablement logic."""
    
    @patch.dict(os.environ, {}, clear=True)
    def test_copy_enabled_in_normal_environment(self):
        """Test that copy is enabled in normal environments."""
        coordinator = DistributedCoordinator()
        assert coordinator.copy_enabled is True
        
        status = coordinator.get_copy_status()
        assert status['copy_enabled'] is True
        assert 'default behavior' in status['reason'].lower()

    @patch.dict(os.environ, {'DISABLE_COPY': 'true'})
    def test_copy_disabled_when_disabled_explicitly(self):
        """Test that copy is disabled when DISABLE_COPY=true."""
        coordinator = DistributedCoordinator()
        assert coordinator.copy_enabled is False
        
        status = coordinator.get_copy_status()
        assert status['copy_enabled'] is False
        assert 'disable_copy=true' in status['reason'].lower()

    @patch.dict(os.environ, {'DISABLE_COPY': 'false'})
    def test_copy_enabled_when_disable_copy_false(self):
        """Test that copy is enabled when DISABLE_COPY=false."""
        coordinator = DistributedCoordinator()
        assert coordinator.copy_enabled is True
        
        status = coordinator.get_copy_status()
        assert status['copy_enabled'] is True
        assert 'default behavior' in status['reason'].lower()

    @patch.dict(os.environ, {'DISABLE_COPY': ''})
    def test_copy_enabled_when_disable_copy_empty(self):
        """Test that copy is enabled when DISABLE_COPY is empty."""
        coordinator = DistributedCoordinator()
        assert coordinator.copy_enabled is True
        
        status = coordinator.get_copy_status()
        assert status['copy_enabled'] is True
        assert 'default behavior' in status['reason'].lower()


class TestSpecialCases:
    """Test special cases and edge conditions."""
    
    @patch.dict(os.environ, {'DISABLE_COPY': ''})  # Empty string
    def test_empty_disable_copy_variable_enables_copy(self):
        """Test that empty DISABLE_COPY variable doesn't disable copy."""
        coordinator = DistributedCoordinator()
        assert coordinator.copy_enabled is True

    @patch.dict(os.environ, {'DISABLE_COPY': 'false'})
    def test_disable_copy_false_enables_copy(self):
        """Test that DISABLE_COPY=false doesn't disable copy."""
        coordinator = DistributedCoordinator()
        assert coordinator.copy_enabled is True

    @patch.dict(os.environ, {'DISABLE_COPY': '0'})
    def test_disable_copy_zero_enables_copy(self):
        """Test that DISABLE_COPY=0 doesn't disable copy."""
        coordinator = DistributedCoordinator()
        assert coordinator.copy_enabled is True

    @patch.dict(os.environ, {'SOME_UNRELATED_VAR': 'true'})  # completely different variable
    def test_unrelated_environment_variables_ignored(self):
        """Test that unrelated environment variables don't affect copy enablement."""
        coordinator = DistributedCoordinator()
        # SOME_UNRELATED_VAR should not disable copy (only DISABLE_COPY works)
        assert coordinator.copy_enabled is True


class TestConsistentBehavior:
    """Test that coordinator behavior is consistent across multiple calls."""
    
    @patch.dict(os.environ, {}, clear=True)
    def test_consistent_copy_enabled_result(self):
        """Test that copy_enabled returns consistent results."""
        coordinator = DistributedCoordinator()
        
        # Multiple calls should return the same result
        first_call = coordinator.copy_enabled
        second_call = coordinator.copy_enabled
        third_call = coordinator.copy_enabled
        
        assert first_call == second_call == third_call

    @patch.dict(os.environ, {'DISABLE_COPY': 'true'})
    def test_consistent_copy_disabled_result(self):
        """Test that copy_enabled returns consistent results when disabled."""
        coordinator = DistributedCoordinator()
        
        # Multiple calls should return the same result
        first_call = coordinator.copy_enabled
        second_call = coordinator.copy_enabled
        third_call = coordinator.copy_enabled
        
        assert first_call == second_call == third_call == False

    def test_consistent_status_information(self, distributed_coordinator):
        """Test that get_copy_status returns consistent information."""
        first_status = distributed_coordinator.get_copy_status()
        second_status = distributed_coordinator.get_copy_status()
        
        assert first_status == second_status
        assert first_status['copy_enabled'] == second_status['copy_enabled']
        assert first_status['reason'] == second_status['reason']


class TestStatusReporting:
    """Test detailed status reporting functionality."""
    
    def test_status_contains_required_fields(self, distributed_coordinator):
        """Test that status dictionary contains all required fields."""
        status = distributed_coordinator.get_copy_status()
        
        required_fields = ['copy_enabled', 'reason']
        for field in required_fields:
            assert field in status
            assert status[field] is not None

    def test_status_reason_is_descriptive(self, distributed_coordinator):
        """Test that status reason provides descriptive information."""
        status = distributed_coordinator.get_copy_status()
        
        reason = status['reason']
        assert isinstance(reason, str)
        assert len(reason) > 0
        # Should contain some descriptive text
        assert any(word in reason.lower() for word in ['copy', 'enabled', 'disabled', 'environment'])

    @patch.dict(os.environ, {'DISABLE_COPY': 'true'})
    def test_disabled_status_explains_why(self):
        """Test that disabled status explains the reason."""
        coordinator = DistributedCoordinator()
        status = coordinator.get_copy_status()
        
        assert status['copy_enabled'] is False
        reason = status['reason'].lower()
        assert 'disable_copy=true' in reason


@pytest.mark.parametrize("env_var,value,expected_disabled", [
    ('DISABLE_COPY', 'true', True),
    ('DISABLE_COPY', 'TRUE', True),
    ('DISABLE_COPY', 'True', True),
    ('DISABLE_COPY', 'false', False),
    ('DISABLE_COPY', '0', False),
    ('DISABLE_COPY', '', False),
    ('SOME_OTHER_VAR', 'true', False),  # Should not disable
])
def test_environment_variable_effects(env_var, value, expected_disabled):
    """Test that specific environment variables have expected effects."""
    with patch.dict(os.environ, {env_var: value}, clear=True):
        coordinator = DistributedCoordinator()
        
        if expected_disabled:
            assert coordinator.copy_enabled is False
            assert coordinator.get_copy_status()['copy_enabled'] is False
        else:
            assert coordinator.copy_enabled is True
            assert coordinator.get_copy_status()['copy_enabled'] is True
