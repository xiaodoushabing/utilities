"""
Test suite for FileIO retry functionality.

These tests validate that the @retry_args decorator works correctly
with FileIO operations, ensuring robustness in the face of temporary failures.

Retry test areas:
🔄 Retry decorator functionality
⏱️ Retry timing and attempts
🚫 Error handling with retries
✅ Success after retry attempts
"""

import pytest
import time
import warnings
from unittest.mock import patch, MagicMock, call

from src.main.file_io import FileIOInterface

pytestmark = pytest.mark.unit


class TestFileIORetryFunctionality:
    """Test retry behavior in FileIO operations."""
    
    def test_retry_decorator_applied_to_interface_methods(self):
        """Test that retry decorator is applied to interface methods.
        
        PYTEST: This test verifies that the retry functionality is properly
        integrated into the FileIO interface methods.
        """
        # Check that methods have the retry_args decorator
        methods_with_retry = ['finfo', 'fread', 'fcopy', 'fwrite', 'fmakedirs', 'fdelete']
        
        for method_name in methods_with_retry:
            method = getattr(FileIOInterface, method_name)
            # The wrapper function should have a __wrapped__ attribute pointing to original
            assert hasattr(method, '__wrapped__'), f"Method {method_name} should have retry decorator"

    @patch('src.main._aux._aux.Retrying')
    def test_retry_with_custom_attempts_and_wait(self, mock_retrying_class, mock_fsspec):
        """Test that custom retry parameters are passed correctly."""
        mock_retrying_instance = MagicMock()
        mock_retrying_class.return_value = mock_retrying_instance
        
        # Mock the retrying instance to call our function
        def mock_retry_call(func, *args, **kwargs):
            return func(*args, **kwargs)
        mock_retrying_instance.side_effect = mock_retry_call
        
        with patch.object(FileIOInterface, '_instantiate') as mock_instantiate:
            mock_fileio = MagicMock()
            mock_fileio._finfo.return_value = {"size": 100}
            mock_instantiate.return_value = mock_fileio
            
            # Call with custom retry parameters
            FileIOInterface.finfo(
                fpath="/test/file.txt",
                max_attempts=5,
                wait=2
            )
            
            # Verify Retrying was configured with custom parameters
            mock_retrying_class.assert_called_once()
            call_kwargs = mock_retrying_class.call_args.kwargs
            
            # Verify stop condition (max attempts)
            assert 'stop' in call_kwargs
            
            # Verify wait condition
            assert 'wait' in call_kwargs

    @patch('src.main._aux._aux.Retrying')
    def test_retry_on_transient_failure_then_success(self, mock_retrying_class, mock_fsspec):
        """Test retry behavior when operation fails then succeeds."""
        # Set up retry mock to actually call function with retry logic
        attempt_count = 0
        
        def mock_retry_call(func, *args, **kwargs):
            nonlocal attempt_count
            max_attempts = 3
            last_exception = None
            
            for attempt in range(max_attempts):
                try:
                    attempt_count += 1
                    result = func(*args, **kwargs)
                    return result  # Success - return the result
                except OSError as e:
                    last_exception = e
                    if attempt == max_attempts - 1:
                        raise e
                    continue
            
            if last_exception:
                raise last_exception
        
        mock_retrying_instance = MagicMock()
        mock_retrying_instance.side_effect = mock_retry_call
        mock_retrying_class.return_value = mock_retrying_instance
        
        with patch.object(FileIOInterface, '_instantiate') as mock_instantiate:
            # Create a mock that fails on first call, succeeds on second
            call_count = 0
            def failing_instantiate(*args, **kwargs):
                nonlocal call_count
                call_count += 1
                if call_count == 1:
                    raise OSError("Temporary network error")
                else:
                    mock_fileio = MagicMock()
                    mock_fileio._finfo.return_value = {"size": 100}
                    return mock_fileio
            
            mock_instantiate.side_effect = failing_instantiate
            
            # This should succeed after retry
            result = FileIOInterface.finfo(fpath="/test/file.txt", max_attempts=3)
            
            # Verify result is returned correctly
            assert result == {"size": 100}
            
            # Verify retry was attempted
            assert attempt_count == 2

    @patch('src.main._aux._aux.Retrying')
    def test_retry_exhausts_attempts_and_reraises(self, mock_retrying_class, mock_fsspec):
        """Test that retry exhausts attempts and re-raises the final error."""
        
        def mock_retry_call(func, *args, **kwargs):
            max_attempts = 2
            last_exception = None
            
            for attempt in range(max_attempts):
                try:
                    result = func(*args, **kwargs)
                    return result
                except OSError as e:
                    last_exception = e
                    if attempt == max_attempts - 1:
                        raise e
                    continue
            
            if last_exception:
                raise last_exception
        
        mock_retrying_instance = MagicMock()
        mock_retrying_instance.side_effect = mock_retry_call
        mock_retrying_class.return_value = mock_retrying_instance
        
        with patch.object(FileIOInterface, '_instantiate') as mock_instantiate:
            # Make _instantiate always fail
            mock_instantiate.side_effect = OSError("Persistent error")
            
            # This should eventually raise the error
            with pytest.raises(OSError, match="Persistent error"):
                FileIOInterface.finfo(fpath="/test/file.txt", max_attempts=2)

    def test_retry_parameters_do_not_interfere_with_normal_operation(self, mock_fsspec):
        """Test that retry parameters don't interfere with normal method parameters."""
        with patch.object(FileIOInterface, '_instantiate') as mock_instantiate:
            mock_fileio = MagicMock()
            mock_fileio._fwrite.return_value = None
            mock_instantiate.return_value = mock_fileio
            
            # Call with both normal and retry parameters
            FileIOInterface.fwrite(
                write_path="/test/file.json",
                data={"test": "data"},
                filesystem="s3",
                max_attempts=3,
                wait=1,
                encoding="utf-8"  # Normal parameter
            )
            
            # Verify normal parameters passed to instantiate
            mock_instantiate.assert_called_once_with(
                fpath="/test/file.json",
                filesystem="s3",
                encoding="utf-8"
            )
            
            # Verify data passed to write method
            mock_fileio._fwrite.assert_called_once_with(
                data={"test": "data"},
                encoding="utf-8"
            )

    @patch('src.main._aux._aux.Retrying', None)  # Simulate tenacity not installed
    def test_retry_gracefully_handles_missing_tenacity(self, mock_fsspec):
        """Test that operations work even when tenacity is not available."""
        with patch.object(FileIOInterface, '_instantiate') as mock_instantiate:
            mock_fileio = MagicMock()
            mock_fileio._finfo.return_value = {"size": 100}
            mock_instantiate.return_value = mock_fileio
            
            # This should work without retry functionality
            result = FileIOInterface.finfo(fpath="/test/file.txt")
            
            # Verify operation still works
            assert result == {"size": 100}
            mock_instantiate.assert_called_once()

    def test_all_interface_methods_support_retry_parameters(self, mock_fsspec):
        """Test that all interface methods accept retry parameters."""
        retry_params = {'max_attempts': 2, 'wait': 0.1}
        
        methods_to_test = [
            ('finfo', {'fpath': '/test/file.txt'}),
            ('fread', {'read_path': '/test/file.txt'}),
            ('fcopy', {'read_path': '/test/source.txt', 'dest_path': '/test/dest.txt'}),
            ('fwrite', {'write_path': '/test/file.txt', 'data': 'test'}),
            ('fmakedirs', {'path': '/test/dir'}),
            ('fdelete', {'path': '/test/file.txt'}),
        ]
        
        for method_name, base_kwargs in methods_to_test:
            with patch.object(FileIOInterface, '_instantiate') as mock_instantiate:
                mock_fileio = MagicMock()
                # Set up appropriate return values
                if method_name == 'finfo':
                    mock_fileio._finfo.return_value = {"size": 100}
                elif method_name == 'fread':
                    mock_fileio._fread.return_value = "data"
                else:
                    getattr(mock_fileio, f"_{method_name.replace('f', '', 1)}").return_value = None
                
                mock_instantiate.return_value = mock_fileio
                
                # Combine base kwargs with retry params
                all_kwargs = {**base_kwargs, **retry_params}
                
                # Call method - should not raise exception
                method = getattr(FileIOInterface, method_name)
                try:
                    # Suppress expected warnings for test operations on non-existent paths
                    with warnings.catch_warnings():
                        warnings.simplefilter("ignore", UserWarning)
                        method(**all_kwargs)
                except Exception as e:
                    pytest.fail(f"Method {method_name} failed with retry params: {e}")


class TestFileIORetryIntegration:
    """Integration tests for retry functionality with real operations."""
    
    @patch('src.main.file_io._base.BaseFileIO._finfo')
    def test_retry_integration_with_real_instantiation(self, mock_finfo, mock_fsspec):
        """Test retry integration with real BaseFileIO instantiation."""
        # Simulate intermittent failure
        attempt_count = 0
        
        def failing_finfo(*args, **kwargs):
            nonlocal attempt_count
            attempt_count += 1
            if attempt_count == 1:
                raise OSError("Network timeout")
            return {"size": 500, "type": "file"}
        
        mock_finfo.side_effect = failing_finfo
        
        # This should succeed after one retry
        result = FileIOInterface.finfo(fpath="/test/file.txt", max_attempts=3, wait=0.1)
        
        # Verify success after retry
        assert result == {"size": 500, "type": "file"}
        assert attempt_count == 2  # Failed once, then succeeded

    def test_retry_timing_behavior(self, mock_fsspec):
        """Test that retry timing behaves as expected."""
        with patch.object(FileIOInterface, '_instantiate') as mock_instantiate:
            # Configure to fail twice then succeed
            attempt_count = 0
            
            def failing_operation(*args, **kwargs):
                nonlocal attempt_count
                attempt_count += 1
                if attempt_count <= 2:
                    raise OSError("Temporary failure")
                return {"size": 100}
            
            mock_fileio = MagicMock()
            mock_fileio._finfo.side_effect = failing_operation
            mock_instantiate.return_value = mock_fileio
            
            # Measure time for retries with wait
            start_time = time.time()
            result = FileIOInterface.finfo(
                fpath="/test/file.txt",
                max_attempts=4,
                wait=0.1  # 100ms wait between retries
            )
            end_time = time.time()
            
            # Verify operation succeeded
            assert result == {"size": 100}
            assert attempt_count == 3  # Failed twice, succeeded third time
            
            # Verify timing (should be at least 200ms for 2 waits)
            # Using loose timing check since test execution can vary
            elapsed = end_time - start_time
            assert elapsed >= 0.15  # Allow some tolerance for execution time
