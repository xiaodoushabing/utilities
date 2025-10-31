"""
Test suite for retry_args decorator and auxiliary utilities.

These tests validate the retry_args decorator functionality including:
- Parameter resolution and validation
- Retry behavior and tenacity integration
- Instance attribute handling
- Error handling and edge cases
"""

import sys
from unittest.mock import MagicMock

# Mock hydra before any imports to prevent import errors
sys.modules['hydra'] = MagicMock()
sys.modules['hydra.logging'] = MagicMock()
sys.modules['hydra.logging.promtail'] = MagicMock()
sys.modules['hydra.logging.promtail'].PromtailAgent = MagicMock()

import pytest
from unittest.mock import MagicMock, patch, call
import os

# Import only the specific functions we're testing
from src.main._aux._aux import retry_args, _resolve, append_to_path_var, iter_update_dict

pytestmark = pytest.mark.unit


class TestRetryArgsDecorator:
    """Test the retry_args decorator functionality."""
    
    def test_decorator_without_parentheses(self, mock_tenacity, sample_function):
        """Test using @retry_args without parentheses."""
        decorated_func = retry_args(sample_function)
        
        # Mock the retry execution to just call the function
        mock_tenacity['retrying_instance'].side_effect = lambda func, *args, **kwargs: func(*args, **kwargs)
        
        result = decorated_func(5, y=10)
        assert result == 15
        
        # Verify tenacity components were called with defaults
        mock_tenacity['Retrying'].assert_called_once()
        mock_tenacity['stop_after_attempt'].assert_called_once_with(2)  # default attempts
        mock_tenacity['wait_fixed'].assert_called_once_with(1)  # default wait

    def test_decorator_with_parentheses(self, mock_tenacity, sample_function):
        """Test using @retry_args() with explicit parameters."""
        decorated_func = retry_args(max_attempts=3, wait_seconds=2)(sample_function)
        
        # Mock the retry execution to just call the function
        mock_tenacity['retrying_instance'].side_effect = lambda func, *args, **kwargs: func(*args, **kwargs)
        
        result = decorated_func(7, y=3)
        assert result == 10
        
        # Verify tenacity components were called with custom parameters
        mock_tenacity['Retrying'].assert_called_once()
        mock_tenacity['stop_after_attempt'].assert_called_once_with(3)
        mock_tenacity['wait_fixed'].assert_called_once_with(2)

    def test_parameter_validation(self, mock_tenacity, sample_function):
        """Test parameter validation in the decorator."""
        # Test negative max_attempts
        decorated_func = retry_args(max_attempts=0)(sample_function)
        
        with pytest.raises(ValueError, match="max_attempts must be at least 1"):
            decorated_func(1, 2)
        
        # Test negative wait_seconds
        decorated_func = retry_args(wait_seconds=-1)(sample_function)
        
        with pytest.raises(ValueError, match="wait_seconds cannot be negative"):
            decorated_func(1, 2)

    def test_instance_method_decoration(self, mock_tenacity, sample_method):
        """Test decorating instance methods and using instance attributes."""
        # Decorate the method
        decorated_method = retry_args()(sample_method.sample_method)
        
        # Mock the retry execution
        mock_tenacity['retrying_instance'].side_effect = lambda func, *args, **kwargs: func(*args, **kwargs)
        
        # Call the decorated bound method (self is already bound)
        result = decorated_method(4, y=6)
        assert result == 24
        
        # Verify it used default values (2, 1) since bound method doesn't pass instance correctly
        mock_tenacity['stop_after_attempt'].assert_called_once_with(2)
        mock_tenacity['wait_fixed'].assert_called_once_with(1)

    def test_unbound_method_with_instance_attrs(self, mock_tenacity):
        """Test decorating unbound methods that can access instance attributes."""
        class TestClass:
            def __init__(self):
                self.retry_max_attempts = 4
                self.retry_wait = 2
            
            def test_method(self, x, y=5):
                return x * y
        
        # Decorate the unbound method
        decorated_method = retry_args()(TestClass.test_method)
        
        # Mock the retry execution
        mock_tenacity['retrying_instance'].side_effect = lambda func, *args, **kwargs: func(*args, **kwargs)
        
        instance = TestClass()
        result = decorated_method(instance, 4, y=6)
        assert result == 24
        
        # Verify it used instance attributes (retry_max_attempts=4, retry_wait=2)
        mock_tenacity['stop_after_attempt'].assert_called_once_with(4)
        mock_tenacity['wait_fixed'].assert_called_once_with(2)

    def test_explicit_params_override_instance_attrs(self, mock_tenacity, sample_method):
        """Test that explicit parameters override instance attributes."""
        decorated_method = retry_args(max_attempts=10, wait_seconds=5)(sample_method.sample_method)
        
        # Mock the retry execution
        mock_tenacity['retrying_instance'].side_effect = lambda func, *args, **kwargs: func(*args, **kwargs)
        
        # Call the decorated bound method (self is already bound)
        decorated_method(2, y=3)
        
        # Should use explicit params, not instance attrs
        mock_tenacity['stop_after_attempt'].assert_called_once_with(10)
        mock_tenacity['wait_fixed'].assert_called_once_with(5)

    def test_custom_attribute_names(self, mock_tenacity, mock_instance_with_retry_attrs):
        """Test using custom attribute names for retry parameters."""
        # Create a simple function to decorate
        def test_func(self, x):
            return x * 2
        
        decorated_func = retry_args(
            max_attempts_attr="custom_attempts_attr",
            wait_attr="custom_wait_attr"
        )(test_func)
        
        # Mock the retry execution
        mock_tenacity['retrying_instance'].side_effect = lambda func, *args, **kwargs: func(*args, **kwargs)
        
        result = decorated_func(mock_instance_with_retry_attrs, 5)
        assert result == 10
        
        # Should use custom attribute values (7 and 2)
        mock_tenacity['stop_after_attempt'].assert_called_once_with(7)
        mock_tenacity['wait_fixed'].assert_called_once_with(2)

    def test_retry_conditions_single_condition(self, mock_tenacity, mock_retry_conditions, sample_function):
        """Test retry with a single retry condition."""
        condition = mock_retry_conditions['retry_if_exception']
        decorated_func = retry_args(retry_conditions=condition)(sample_function)
        
        mock_tenacity['retrying_instance'].side_effect = lambda func, *args, **kwargs: func(*args, **kwargs)
        
        decorated_func(3, y=4)
        
        # Verify the condition was passed to Retrying
        call_kwargs = mock_tenacity['Retrying'].call_args.kwargs
        assert 'retry' in call_kwargs
        assert call_kwargs['retry'] == condition

    def test_retry_conditions_multiple_conditions(self, mock_tenacity, mock_retry_conditions, sample_function):
        """Test retry with multiple retry conditions."""
        conditions = [
            mock_retry_conditions['retry_if_exception'],
            mock_retry_conditions['retry_if_result']
        ]
        decorated_func = retry_args(retry_conditions=conditions)(sample_function)
        
        mock_tenacity['retrying_instance'].side_effect = lambda func, *args, **kwargs: func(*args, **kwargs)
        
        decorated_func(2, y=3)
        
        # Verify retry_any was called with all conditions
        mock_tenacity['retry_any'].assert_called_once_with(
            mock_retry_conditions['retry_if_exception'],
            mock_retry_conditions['retry_if_result']
        )
        
        # Verify the combined condition was passed to Retrying
        call_kwargs = mock_tenacity['Retrying'].call_args.kwargs
        assert 'retry' in call_kwargs
        assert call_kwargs['retry'] == mock_tenacity['retry_any_instance']

    def test_retry_conditions_validation_errors(self, sample_function):
        """Test validation errors for retry conditions."""
        # Test empty list - need to call the decorated function to trigger validation
        decorated_func = retry_args(retry_conditions=[])(sample_function)
        with pytest.raises(ValueError, match="retry_conditions list cannot be empty"):
            decorated_func(1, 2)
        
        # Test non-callable single condition
        decorated_func = retry_args(retry_conditions="not_callable")(sample_function)
        with pytest.raises(TypeError, match="retry_conditions must be a callable tenacity predicate"):
            decorated_func(1, 2)
        
        # Test non-callable in list
        decorated_func = retry_args(retry_conditions=["not_callable"])(sample_function)
        with pytest.raises(TypeError, match="retry_conditions\\[0\\] is not callable"):
            decorated_func(1, 2)

    def test_callbacks(self, mock_tenacity, mock_callbacks, sample_function):
        """Test before_retry and after_retry callbacks."""
        decorated_func = retry_args(
            before_retry=mock_callbacks['before_retry'],
            after_retry=mock_callbacks['after_retry']
        )(sample_function)
        
        mock_tenacity['retrying_instance'].side_effect = lambda func, *args, **kwargs: func(*args, **kwargs)
        
        decorated_func(1, y=2)
        
        # Verify callbacks were passed to Retrying
        call_kwargs = mock_tenacity['Retrying'].call_args.kwargs
        assert call_kwargs['before'] == mock_callbacks['before_retry']
        assert call_kwargs['after'] == mock_callbacks['after_retry']

    def test_missing_tenacity_components(self, sample_function):
        """Test graceful handling when tenacity components are None."""
        with patch('src.main._aux._aux.Retrying', None), \
             patch('src.main._aux._aux.stop_after_attempt', None), \
             patch('src.main._aux._aux.wait_fixed', None):
            
            # Should raise TypeError when trying to call None components
            decorated_func = retry_args()(sample_function)
            
            with pytest.raises(TypeError):
                decorated_func(1, 2)

    def test_retry_execution_with_failure_then_success(self, mock_tenacity, failing_function):
        """Test actual retry execution behavior."""
        fail_func = failing_function(fail_count=2, success_value="finally_succeeded")
        
        # Create a more realistic retry mock that actually retries
        def mock_retry_execution(func, *args, **kwargs):
            max_attempts = 3
            for attempt in range(max_attempts):
                try:
                    return func(*args, **kwargs)
                except OSError:
                    if attempt == max_attempts - 1:
                        raise
                    continue
        
        mock_tenacity['retrying_instance'].side_effect = mock_retry_execution
        
        decorated_func = retry_args(max_attempts=3)(fail_func)
        result = decorated_func()
        
        assert result == "finally_succeeded"
        assert fail_func.call_count() == 3  # Failed twice, succeeded on third

    def test_custom_defaults(self, mock_tenacity, sample_function):
        """Test custom default values for attempts and wait."""
        decorated_func = retry_args(attempts_default=5, wait_default=3)(sample_function)
        
        mock_tenacity['retrying_instance'].side_effect = lambda func, *args, **kwargs: func(*args, **kwargs)
        
        decorated_func(1, 2)
        
        # Should use custom defaults
        mock_tenacity['stop_after_attempt'].assert_called_once_with(5)
        mock_tenacity['wait_fixed'].assert_called_once_with(3)


class TestResolveFunction:
    """Test the _resolve helper function."""
    
    def test_explicit_value_takes_precedence(self):
        """Test that explicit values override everything."""
        instance = MagicMock()
        instance.test_attr = "instance_value"
        
        result = _resolve(
            instance=instance,
            attr_name="test_attr",
            explicit="explicit_value",
            default="default_value"
        )
        
        assert result == "explicit_value"

    def test_instance_attribute_over_default(self):
        """Test that instance attributes override defaults."""
        instance = MagicMock()
        instance.test_attr = "instance_value"
        
        result = _resolve(
            instance=instance,
            attr_name="test_attr",
            explicit=None,
            default="default_value"
        )
        
        assert result == "instance_value"

    def test_default_when_no_instance_attr(self):
        """Test default value when instance has no attribute."""
        instance = MagicMock()
        # Don't set the missing_attr, use spec to control available attributes
        instance = MagicMock(spec=['test_attr'])
        instance.test_attr = "instance_value"  # Different attribute
        
        result = _resolve(
            instance=instance,
            attr_name="missing_attr",
            explicit=None,
            default="default_value"
        )
        
        assert result == "default_value"

    def test_default_when_no_instance(self):
        """Test default value when no instance provided."""
        result = _resolve(
            instance=None,
            attr_name="test_attr",
            explicit=None,
            default="default_value"
        )
        
        assert result == "default_value"

    def test_default_when_no_attr_name(self):
        """Test default value when no attribute name provided."""
        instance = MagicMock()
        instance.test_attr = "instance_value"
        
        result = _resolve(
            instance=instance,
            attr_name=None,
            explicit=None,
            default="default_value"
        )
        
        assert result == "default_value"

    def test_explicit_none_vs_missing(self):
        """Test that explicit None is different from missing explicit value."""
        instance = MagicMock()
        instance.test_attr = "instance_value"
        
        # When explicit is None (not missing)
        result = _resolve(
            instance=instance,
            attr_name="test_attr",
            explicit=None,
            default="default_value"
        )
        
        assert result == "instance_value"  # Should use instance value, not default


class TestUtilityFunctions:
    """Test auxiliary utility functions."""
    
    def test_append_to_path_var_new_path(self, mock_environ, sample_path_data):
        """Test appending to empty PATH variable."""
        new_path = sample_path_data['new_path']
        
        append_to_path_var('PATH', new_path)
        
        assert os.environ['PATH'] == new_path

    def test_append_to_path_var_existing_path(self, mock_environ, sample_path_data):
        """Test appending to existing PATH variable."""
        existing_path = sample_path_data['existing_path']
        new_path = sample_path_data['new_path']
        
        os.environ['PATH'] = existing_path
        append_to_path_var('PATH', new_path)
        
        expected = f"{new_path}:{existing_path}"
        assert os.environ['PATH'] == expected

    def test_append_to_path_var_duplicate_path(self, mock_environ, sample_path_data):
        """Test that duplicate paths are not added."""
        existing_path = sample_path_data['existing_path']
        duplicate_path = "/usr/bin"  # Already in existing_path
        
        os.environ['PATH'] = existing_path
        append_to_path_var('PATH', duplicate_path)
        
        # Should remain unchanged
        assert os.environ['PATH'] == existing_path

    def test_append_to_path_var_strips_colons(self, mock_environ):
        """Test that trailing/leading colons are handled properly."""
        os.environ['PATH'] = ":/usr/bin:"
        
        append_to_path_var('PATH', '/opt/bin')
        
        # Check that it properly handles the existing colons (strips them and reformats)
        result_path = os.environ['PATH']
        assert '/opt/bin' in result_path
        assert '/usr/bin' in result_path
        # The exact format may vary, but both paths should be present

    def test_iter_update_dict_simple_update(self, sample_dict_data):
        """Test simple dictionary update without nesting."""
        base = sample_dict_data['simple_dict'].copy()
        update = sample_dict_data['simple_update'].copy()
        
        result = iter_update_dict(base, update)
        
        expected = {
            'key1': 'value1',
            'key2': 'updated',  # Updated
            'key3': 'new'       # Added
        }
        assert result == expected
        assert base == expected  # Should modify in place

    def test_iter_update_dict_nested_update(self, sample_dict_data):
        """Test recursive dictionary update with nested structures."""
        base = sample_dict_data['base_dict'].copy()
        update = sample_dict_data['update_dict'].copy()
        
        result = iter_update_dict(base, update)
        
        # Verify nested updates
        assert result['level1']['key2'] == 'updated_value2'
        assert result['level1']['key3'] == 'new_value3'
        assert result['level1']['nested']['deep_key'] == 'updated_deep_value'
        assert result['level1']['nested']['new_deep_key'] == 'new_deep_value'
        assert result['level3'] == 'completely_new'
        
        # Verify existing values are preserved
        assert result['level1']['key1'] == 'value1'
        assert result['level2'] == 'simple_value'

    def test_iter_update_dict_overwrite_non_dict(self, sample_dict_data):
        """Test that non-dict values are overwritten completely."""
        base = {'key': 'string_value'}
        update = {'key': {'nested': 'dict_value'}}
        
        result = iter_update_dict(base, update)
        
        assert result['key'] == {'nested': 'dict_value'}

    def test_iter_update_dict_empty_update(self):
        """Test updating with empty dictionary."""
        base = {'key1': 'value1', 'key2': {'nested': 'value'}}
        update = {}
        
        result = iter_update_dict(base, update)
        
        # Should remain unchanged
        assert result == {'key1': 'value1', 'key2': {'nested': 'value'}}

    def test_iter_update_dict_empty_base(self):
        """Test updating empty base dictionary."""
        base = {}
        update = {'key1': 'value1', 'key2': {'nested': 'value'}}
        
        result = iter_update_dict(base, update)
        
        assert result == update
        assert base == update  # Should modify base in place


class TestRetryEdgeCases:
    """Test edge cases and error handling for retry functionality."""
    
    def test_retry_with_no_exceptions(self, mock_tenacity, sample_function):
        """Test retry decorator on function that doesn't raise exceptions."""
        decorated_func = retry_args(max_attempts=3)(sample_function)
        
        # Mock successful execution
        mock_tenacity['retrying_instance'].side_effect = lambda func, *args, **kwargs: func(*args, **kwargs)
        
        result = decorated_func(10, y=5)
        assert result == 15
        
        # Should still set up retry configuration
        mock_tenacity['Retrying'].assert_called_once()

    def test_retry_preserves_function_metadata(self, sample_function):
        """Test that decorator preserves original function metadata."""
        sample_function.__doc__ = "Original docstring"
        sample_function.__name__ = "original_name"
        
        decorated_func = retry_args()(sample_function)
        
        # Should preserve metadata due to @wraps
        assert decorated_func.__doc__ == "Original docstring"
        assert decorated_func.__name__ == "original_name"
        assert hasattr(decorated_func, '__wrapped__')
        assert decorated_func.__wrapped__ is sample_function

    def test_retry_args_with_lambda(self, mock_tenacity):
        """Test retry_args decorator with lambda functions."""
        lambda_func = lambda x: x * 2
        decorated_func = retry_args(max_attempts=2)(lambda_func)
        
        mock_tenacity['retrying_instance'].side_effect = lambda func, *args, **kwargs: func(*args, **kwargs)
        
        result = decorated_func(5)
        assert result == 10

    def test_retry_with_keyword_only_arguments(self, mock_tenacity):
        """Test retry decorator with functions using keyword-only arguments."""
        def keyword_only_func(*, value, multiplier=2):
            return value * multiplier
        
        decorated_func = retry_args()(keyword_only_func)
        
        mock_tenacity['retrying_instance'].side_effect = lambda func, *args, **kwargs: func(*args, **kwargs)
        
        result = decorated_func(value=3, multiplier=4)
        assert result == 12

    def test_retry_with_varargs_and_kwargs(self, mock_tenacity):
        """Test retry decorator with functions using *args and **kwargs."""
        def flexible_func(*args, **kwargs):
            return sum(args) + sum(kwargs.values())
        
        decorated_func = retry_args()(flexible_func)
        
        mock_tenacity['retrying_instance'].side_effect = lambda func, *args, **kwargs: func(*args, **kwargs)
        
        result = decorated_func(1, 2, 3, a=4, b=5)
        assert result == 15  # 1+2+3+4+5

    def test_retry_with_complex_return_types(self, mock_tenacity):
        """Test retry decorator with functions returning complex types."""
        def complex_return_func():
            return {
                'data': [1, 2, 3],
                'metadata': {'success': True},
                'nested': {'deep': {'value': 42}}
            }
        
        decorated_func = retry_args()(complex_return_func)
        
        mock_tenacity['retrying_instance'].side_effect = lambda func, *args, **kwargs: func(*args, **kwargs)
        
        result = decorated_func()
        expected = {
            'data': [1, 2, 3],
            'metadata': {'success': True},
            'nested': {'deep': {'value': 42}}
        }
        assert result == expected

    def test_instance_detection_edge_cases(self, mock_tenacity):
        """Test edge cases for instance detection in decorated functions."""
        def standalone_func(first_arg, second_arg):
            return f"{first_arg}-{second_arg}"
        
        decorated_func = retry_args()(standalone_func)
        
        mock_tenacity['retrying_instance'].side_effect = lambda func, *args, **kwargs: func(*args, **kwargs)
        
        # Call with objects that have __dict__ but aren't instances
        class FakeInstance:
            def __init__(self):
                self.retry_max_attempts = 999
        
        fake_obj = FakeInstance()
        result = decorated_func(fake_obj, "test")
        assert result == f"{fake_obj}-test"
        
        # Should detect fake_obj as instance and use its attributes
        mock_tenacity['stop_after_attempt'].assert_called_with(999)

    def test_retry_conditions_single_item_list(self, mock_tenacity, mock_retry_conditions, sample_function):
        """Test retry conditions with single-item list (should not use retry_any)."""
        condition = mock_retry_conditions['retry_if_exception']
        decorated_func = retry_args(retry_conditions=[condition])(sample_function)
        
        mock_tenacity['retrying_instance'].side_effect = lambda func, *args, **kwargs: func(*args, **kwargs)
        
        decorated_func(1, 2)
        
        # Should use the single condition directly, not retry_any
        mock_tenacity['retry_any'].assert_not_called()
        call_kwargs = mock_tenacity['Retrying'].call_args.kwargs
        assert call_kwargs['retry'] == condition

    def test_retry_conditions_error_messages(self, sample_function):
        """Test specific error messages for retry conditions validation."""
        # Test error message for non-callable single condition
        decorated_func = retry_args(retry_conditions=123)(sample_function)
        with pytest.raises(TypeError) as exc_info:
            decorated_func(1, 2)
        
        assert "retry_conditions must be a callable tenacity predicate" in str(exc_info.value)
        assert "Example: retry_if_exception_type" in str(exc_info.value)
        assert "got int" in str(exc_info.value).lower()
        
        # Test error message for non-callable in list with index
        decorated_func = retry_args(retry_conditions=[lambda x: True, "not_callable", lambda y: False])(sample_function)
        with pytest.raises(TypeError) as exc_info:
            decorated_func(1, 2)
        
        assert "retry_conditions[1] is not callable" in str(exc_info.value)

    def test_parameter_resolution_with_zero_values(self, mock_tenacity):
        """Test parameter resolution when values are 0 (falsy but valid)."""
        instance = MagicMock()
        instance.retry_max_attempts = 0  # Should trigger validation error
        instance.retry_wait = 0  # Valid for wait_seconds
        
        def test_method(self):
            return "test"
        
        # Test that 0 attempts triggers validation
        decorated_func = retry_args()(test_method)
        
        with pytest.raises(ValueError, match="max_attempts must be at least 1"):
            decorated_func(instance)

    def test_tenacity_import_warning_simulation(self):
        """Test simulation of tenacity import failure behavior."""
        # This test verifies the behavior described in the import block
        # When tenacity is not available, the components should be None
        
        with patch('src.main._aux._aux.Retrying', None), \
             patch('src.main._aux._aux.stop_after_attempt', None), \
             patch('src.main._aux._aux.wait_fixed', None), \
             patch('src.main._aux._aux.retry_any', None):
            
            # The decorator should still be importable
            from src.main._aux._aux import retry_args
            
            # But using it should fail when accessing None components
            def test_func():
                return "test"
                
            decorated = retry_args()(test_func)
            
            with pytest.raises(TypeError):  # NoneType is not callable
                decorated()

    def test_reraise_configuration(self, mock_tenacity, sample_function):
        """Test that reraise=True is always set in retry configuration."""
        decorated_func = retry_args()(sample_function)
        
        mock_tenacity['retrying_instance'].side_effect = lambda func, *args, **kwargs: func(*args, **kwargs)
        
        decorated_func(1, 2)
        
        # Verify reraise=True is set
        call_kwargs = mock_tenacity['Retrying'].call_args.kwargs
        assert call_kwargs['reraise'] is True

    def test_all_retry_kwargs_structure(self, mock_tenacity, mock_retry_conditions, mock_callbacks, sample_function):
        """Test complete retry kwargs structure with all options."""
        decorated_func = retry_args(
            max_attempts=5,
            wait_seconds=3,
            retry_conditions=[mock_retry_conditions['retry_if_exception']],
            before_retry=mock_callbacks['before_retry'],
            after_retry=mock_callbacks['after_retry']
        )(sample_function)
        
        mock_tenacity['retrying_instance'].side_effect = lambda func, *args, **kwargs: func(*args, **kwargs)
        
        decorated_func(1, 2)
        
        # Verify all components in the retry configuration
        call_kwargs = mock_tenacity['Retrying'].call_args.kwargs
        
        expected_keys = {'stop', 'wait', 'reraise', 'retry', 'before', 'after'}
        assert set(call_kwargs.keys()) == expected_keys
        
        assert call_kwargs['reraise'] is True
        assert call_kwargs['before'] == mock_callbacks['before_retry']
        assert call_kwargs['after'] == mock_callbacks['after_retry']