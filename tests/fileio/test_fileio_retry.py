"""
Test suite for FileIO retry functionality.

These tests validate that the @retry_args decorator works correctly
with FileIO operations, ensuring robustness in the face of temporary failures.
"""

import pytest
from unittest.mock import patch, MagicMock

from src.main.file_io import FileIOInterface

pytestmark = pytest.mark.unit


class TestFileIORetryFunctionality:
    """Test retry behavior in FileIO operations."""
    
    @pytest.mark.parametrize("method_name", [
        'finfo', 'fread', 'fcopy', 'fwrite', 'fmakedirs', 'fdelete'
    ])
    def test_retry_decorator_applied_to_interface_methods(self, method_name):
        """Test that retry decorator is applied to interface methods."""
        method = getattr(FileIOInterface, method_name)
        # The wrapper function should have a __wrapped__ attribute pointing to original
        assert hasattr(method, '__wrapped__'), f"Method {method_name} should have retry decorator"

    def test_retry_with_custom_attempts_and_wait(self, mock_retry_decorator, mock_instantiate):
        """Test that custom retry parameters are passed correctly."""
        mock_instantiate['fileio']._finfo.return_value = {"size": 100}

        # Call with custom retry parameters
        FileIOInterface.finfo(
            fpath="/test/file.txt",
            max_attempts=5,
            wait=2
        )
        
        # Verify Retrying was configured with custom parameters
        mock_retry_decorator.assert_called_once()
        call_kwargs = mock_retry_decorator.call_args.kwargs
        
        # Verify stop and wait conditions were set
        assert 'stop' in call_kwargs
        assert 'wait' in call_kwargs

    def test_retry_on_transient_failure_then_success(self, mock_fsspec, mock_instantiate, failing_then_succeeding_operation):
        """Test retry behavior when operation fails then succeeds."""
        
        def mock_retry_call(func, *args, **kwargs):
            # Simulate retry logic
            max_attempts = 3
            last_exception = None

            for attempt in range(1, max_attempts + 1):
                try:
                    result = failing_then_succeeding_operation(*args, **kwargs)
                    return result  # Success - return the result
                except OSError as e:
                    last_exception = e
                    if attempt == max_attempts:
                        raise e
                    continue
            
            if last_exception:
                raise last_exception
        
        with patch('src.main._aux._aux.Retrying') as mock_retrying_class:
            mock_retrying_instance = MagicMock()
            mock_retrying_instance.side_effect = mock_retry_call
            mock_retrying_class.return_value = mock_retrying_instance
            
            with patch.object(FileIOInterface, '_instantiate') as mock_instantiate:
                mock_fileio = MagicMock()
                mock_fileio._finfo.side_effect = failing_then_succeeding_operation
                mock_instantiate.return_value = mock_fileio
                
                # This should succeed after retries
                result = FileIOInterface.finfo(fpath="/test/file.txt")
                assert result == "Success"

    @patch('src.main._aux._aux.Retrying', None)  # Simulate tenacity not installed
    def test_retry_gracefully_handles_missing_tenacity(self, mock_fsspec, mock_instantiate):
        """Test that operations work even when tenacity is not available."""
        mock_instantiate['fileio']._finfo.return_value = {"size": 100}
        
        # Should work without retry functionality
        result = FileIOInterface.finfo(fpath="/test/file.txt")
        assert result == {"size": 100}

    @pytest.mark.parametrize("method_name,default_args", [
        ('finfo', {'fpath': '/test/file.txt'}),
        ('fread', {'read_path': '/test/file.txt'}),
        ('fwrite', {'write_path': '/test/file.txt', 'data': 'test'}),
        ('fdelete', {'path': '/test/file.txt'}),
        ('fmakedirs', {'path': '/test/dir'}),
        ('fcopy', {'read_path': '/test/source.txt', 'dest_path': '/test/dest.txt'})
    ])
    def test_all_interface_methods_support_retry_parameters(self, mock_fsspec, method_name, default_args):
        """Test that all interface methods accept retry parameters."""
        # Methods that use _instantiate vs those that work directly with UPath
        instantiate_methods = {'finfo', 'fread', 'fwrite', 'fcopy'}
        direct_methods = {'fdelete', 'fmakedirs'}
        
        if method_name in instantiate_methods:
            with patch.object(FileIOInterface, '_instantiate') as mock_instantiate:
                mock_fileio = MagicMock()
                # Configure appropriate return values for different methods
                if method_name == 'finfo':
                    mock_fileio._finfo.return_value = {"size": 100}
                elif method_name == 'fread':
                    mock_fileio._fread.return_value = "file content"
                elif method_name == 'fwrite':
                    mock_fileio._fwrite.return_value = None
                elif method_name == 'fcopy':
                    mock_fileio._fcopy.return_value = None
                
                mock_instantiate.return_value = mock_fileio
                
                # Call method with retry parameters
                method = getattr(FileIOInterface, method_name)
                method(**default_args, max_attempts=3, wait=1)
                
        elif method_name in direct_methods:
            # For methods that work directly with UPath
            with patch('src.main.file_io.UPath') as mock_upath_class:
                mock_upath = MagicMock()
                if method_name == 'fmakedirs':
                    mock_upath.fs.makedirs = MagicMock()
                elif method_name == 'fdelete':
                    mock_upath.is_dir.return_value = True  # Assume directory for simplicity
                    mock_upath.path = default_args['path']
                    mock_upath.fs.rm = MagicMock()
                mock_upath_class.return_value = mock_upath
                
                # Call method with retry parameters
                method = getattr(FileIOInterface, method_name)
                method(**default_args, max_attempts=3, wait=1)
        
        # Should not raise any errors
        assert True  # Test passes if no exception is raised

    def test_retry_preserves_original_exception_on_final_failure(self, mock_fsspec):
        """Test that the original exception is preserved when all retries fail."""
        
        def always_failing_operation(*args, **kwargs):
            raise OSError("Persistent failure")
        
        def mock_retry_call(func, *args, **kwargs):
            # Simulate retry logic that eventually fails
            max_attempts = 3
            
            for attempt in range(max_attempts):
                try:
                    return func(*args, **kwargs)
                except OSError as e:
                    if attempt == max_attempts - 1:
                        raise e
                    continue
        
        with patch('src.main._aux._aux.Retrying') as mock_retrying_class:
            mock_retrying_instance = MagicMock()
            mock_retrying_instance.side_effect = mock_retry_call
            mock_retrying_class.return_value = mock_retrying_instance
            
            with patch.object(FileIOInterface, '_instantiate') as mock_instantiate:
                mock_fileio = MagicMock()
                mock_fileio._finfo.side_effect = always_failing_operation
                mock_instantiate.return_value = mock_fileio
                
                # Should raise the original OSError
                with pytest.raises(OSError, match="Persistent failure"):
                    FileIOInterface.finfo(fpath="/test/file.txt")
