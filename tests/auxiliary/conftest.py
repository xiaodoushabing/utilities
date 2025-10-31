"""
Shared pytest fixtures for auxiliary utilities test suite.

This file centralizes common test setup and teardown logic for testing
the auxiliary utilities including retry functionality, path operations,
and utility functions.
"""

import pytest
import os
from unittest.mock import MagicMock, patch


# ========================================================================================
# RETRY TESTING FIXTURES
# ========================================================================================

@pytest.fixture
def mock_tenacity():
    """
    Mock tenacity library components for testing retry functionality.
    
    Returns a dict with all mocked tenacity components needed for retry_args testing.
    """
    with patch('src.main._aux._aux.Retrying') as mock_retrying, \
         patch('src.main._aux._aux.stop_after_attempt') as mock_stop, \
         patch('src.main._aux._aux.wait_fixed') as mock_wait, \
         patch('src.main._aux._aux.retry_any') as mock_retry_any:
        
        # Mock Retrying class
        mock_retrying_instance = MagicMock()
        mock_retrying.return_value = mock_retrying_instance
        
        # Mock retry conditions
        mock_stop_instance = MagicMock()
        mock_stop.return_value = mock_stop_instance
        
        mock_wait_instance = MagicMock()
        mock_wait.return_value = mock_wait_instance
        
        mock_retry_any_instance = MagicMock()
        mock_retry_any.return_value = mock_retry_any_instance
        
        yield {
            'Retrying': mock_retrying,
            'stop_after_attempt': mock_stop,
            'wait_fixed': mock_wait,
            'retry_any': mock_retry_any,
            'retrying_instance': mock_retrying_instance,
            'stop_instance': mock_stop_instance,
            'wait_instance': mock_wait_instance,
            'retry_any_instance': mock_retry_any_instance
        }


@pytest.fixture
def mock_retry_conditions():
    """
    Mock retry condition functions for testing.
    """
    retry_if_exception = MagicMock()
    retry_if_result = MagicMock()
    
    # Make them callable and identifiable
    retry_if_exception.__name__ = 'retry_if_exception_type'
    retry_if_result.__name__ = 'retry_if_result'
    
    return {
        'retry_if_exception': retry_if_exception,
        'retry_if_result': retry_if_result
    }


@pytest.fixture
def failing_function():
    """
    Creates a function that fails a specified number of times then succeeds.
    """
    def create_failing_function(fail_count=2, exception_type=OSError, success_value="success"):
        call_count = 0
        
        def function(*args, **kwargs):
            nonlocal call_count
            call_count += 1
            if call_count <= fail_count:
                raise exception_type(f"Attempt {call_count} failed")
            return success_value
        
        function.call_count = lambda: call_count
        function.reset = lambda: setattr(function, 'call_count', lambda: 0) or globals().update({'call_count': 0})
        
        return function
    
    return create_failing_function


@pytest.fixture
def mock_instance_with_retry_attrs():
    """
    Create a mock instance that has retry-related attributes.
    """
    instance = MagicMock()
    instance.retry_max_attempts = 5
    instance.retry_wait = 3
    instance.custom_attempts_attr = 7
    instance.custom_wait_attr = 2
    return instance


@pytest.fixture
def sample_function():
    """
    A simple function for testing decorators.
    """
    def simple_function(x, y=10):
        return x + y
    
    return simple_function


@pytest.fixture
def sample_method():
    """
    A simple method for testing decorators on class methods.
    """
    class SampleClass:
        def __init__(self):
            self.retry_max_attempts = 4
            self.retry_wait = 2
        
        def sample_method(self, x, y=5):
            return x * y
    
    return SampleClass()


# ========================================================================================
# PATH TESTING FIXTURES
# ========================================================================================

@pytest.fixture
def mock_environ():
    """
    Mock os.environ for testing path operations.
    """
    with patch.dict(os.environ, {}, clear=True):
        yield os.environ


@pytest.fixture
def sample_path_data():
    """
    Sample data for testing path operations.
    """
    return {
        'existing_path': '/usr/bin:/usr/local/bin',
        'new_path': '/opt/myapp/bin',
        'empty_path': '',
        'path_with_colons': ':/usr/bin:',
    }


# ========================================================================================
# DICTIONARY UPDATE TESTING FIXTURES
# ========================================================================================

@pytest.fixture
def sample_dict_data():
    """
    Sample dictionary data for testing iter_update_dict.
    """
    return {
        'base_dict': {
            'level1': {
                'key1': 'value1',
                'key2': 'value2',
                'nested': {
                    'deep_key': 'deep_value'
                }
            },
            'level2': 'simple_value'
        },
        'update_dict': {
            'level1': {
                'key2': 'updated_value2',
                'key3': 'new_value3',
                'nested': {
                    'deep_key': 'updated_deep_value',
                    'new_deep_key': 'new_deep_value'
                }
            },
            'level3': 'completely_new'
        },
        'simple_dict': {
            'key1': 'value1',
            'key2': 'value2'
        },
        'simple_update': {
            'key2': 'updated',
            'key3': 'new'
        }
    }


# ========================================================================================
# CALLBACK TESTING FIXTURES
# ========================================================================================

@pytest.fixture
def mock_callbacks():
    """
    Mock callback functions for testing retry callbacks.
    """
    before_retry = MagicMock()
    after_retry = MagicMock()
    
    return {
        'before_retry': before_retry,
        'after_retry': after_retry
    }


# ========================================================================================
# ERROR TESTING FIXTURES
# ========================================================================================

@pytest.fixture
def custom_exceptions():
    """
    Custom exception classes for testing error handling.
    """
    class CustomError(Exception):
        pass
    
    class NetworkError(Exception):
        pass
    
    class RetryableError(Exception):
        pass
    
    return {
        'CustomError': CustomError,
        'NetworkError': NetworkError,
        'RetryableError': RetryableError
    }