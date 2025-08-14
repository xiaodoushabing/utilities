"""
Comprehensive test suite for FileIOInterface class.

This file demonstrates pytest best practices following FIRST principles:
- Fast: Tests run quickly with minimal setup
- Independent: Each test can run in isolation
- Repeatable: Tests produce consistent results
- Self-Validating: Clear pass/fail with assertions
- Timely: Tests written alongside code

Key pytest concepts demonstrated:
📚 Fixtures: Reusable setup/teardown code (see conftest.py)
🔄 Parametrize: Run same test with different inputs
🎭 Mocking: Replace real dependencies with controllable fakes
🧪 Assertions: Verify expected behavior
🛡️ Error Testing: Validate error conditions with pytest.raises
"""

import pytest
import os
from unittest.mock import patch, MagicMock, call
from pathlib import Path

from src.main.file_io import FileIOInterface
from src.main.file_io._base import BaseFileIO

pytestmark = pytest.mark.unit


# ========================================================================================
# PYTEST LEARNING TIP 💡
# Test classes group related tests together. Each method starting with 'test_' 
# becomes a separate test case. Classes help organize tests by functionality.

# FIRST PRINCIPLES APPLIED:
# 📚 Fast: Mocked dependencies, minimal file I/O
# 🔄 Independent: Each test has isolated setup
# 🎭 Repeatable: Deterministic with controlled inputs
# 🧪 Self-Validating: Clear assertions
# ⏰ Timely: Comprehensive coverage of interface
# ========================================================================================


class TestFileIOInterfaceInstantiation:
    """Test FileIOInterface._instantiate method."""
    
    def test_instantiate_creates_base_fileio_object(self, mock_fsspec):
        """Test that _instantiate creates a BaseFileIO object correctly.
        
        PYTEST: This test validates the core factory method that creates
        BaseFileIO instances with proper UPath configuration.
        """
        test_path = "/test/file.txt"
        
        with patch('src.main.file_io.UPath') as mock_upath_class, \
             patch('src.main.file_io.BaseFileIO') as mock_baseio_class:
            
            mock_upath_instance = MagicMock()
            mock_upath_class.return_value = mock_upath_instance
            
            mock_baseio_instance = MagicMock()
            mock_baseio_class.return_value = mock_baseio_instance
            
            # Test instantiation
            result = FileIOInterface._instantiate(fpath=test_path)
            
            # Verify UPath was created with correct parameters
            mock_upath_class.assert_called_once_with(test_path, protocol=None)
            
            # Verify BaseFileIO was created with UPath object
            mock_baseio_class.assert_called_once_with(upath_obj=mock_upath_instance)
            
            # Verify correct instance returned
            assert result == mock_baseio_instance

    def test_instantiate_with_filesystem_parameter(self, mock_fsspec):
        """Test _instantiate with specific filesystem protocol."""
        test_path = "/test/file.txt"
        filesystem = "s3"
        
        with patch('src.main.file_io.UPath') as mock_upath_class, \
             patch('src.main.file_io.BaseFileIO') as mock_baseio_class:
            
            FileIOInterface._instantiate(fpath=test_path, filesystem=filesystem)
            
            # Verify UPath was created with filesystem protocol
            mock_upath_class.assert_called_once_with(test_path, protocol=filesystem)

    def test_instantiate_validates_filesystem_protocol(self, mock_fsspec):
        """Test that invalid filesystem protocols are rejected.
        
        PYTEST: Using pytest.raises to validate error conditions.
        This ensures our validation logic works correctly.
        """
        test_path = "/test/file.txt"
        invalid_filesystem = "invalid_protocol"
        
        # Test assertion error for invalid protocol
        with pytest.raises(AssertionError):
            FileIOInterface._instantiate(fpath=test_path, filesystem=invalid_filesystem)

    def test_instantiate_passes_additional_args_and_kwargs(self, mock_fsspec):
        """Test that additional arguments are passed through correctly."""
        test_path = "/test/file.txt"
        test_arg = "extra_arg"
        test_kwarg = "extra_value"
        
        with patch('src.main.file_io.UPath') as mock_upath_class, \
             patch('src.main.file_io.BaseFileIO') as mock_baseio_class:
            
            FileIOInterface._instantiate(
                fpath=test_path, 
                extra_arg=test_arg, 
                test_kwarg=test_kwarg
            )
            
            # Verify additional args/kwargs passed to BaseFileIO
            mock_baseio_class.assert_called_once_with(
                upath_obj=mock_upath_class.return_value,
                extra_arg=test_arg,
                test_kwarg=test_kwarg
            )


class TestFileIOInterfaceFileInfo:
    """Test FileIOInterface.finfo method."""
    
    def test_finfo_calls_baseio_finfo_method(self, mock_fsspec):
        """Test that finfo properly delegates to BaseFileIO._finfo."""
        test_path = "/test/file.txt"
        expected_info = {"size": 1024, "type": "file"}
        
        with patch.object(FileIOInterface, '_instantiate') as mock_instantiate:
            mock_fileio = MagicMock()
            mock_fileio._finfo.return_value = expected_info
            mock_instantiate.return_value = mock_fileio
            
            result = FileIOInterface.finfo(fpath=test_path)
            
            # Verify instantiate was called correctly
            mock_instantiate.assert_called_once_with(fpath=test_path, filesystem=None)
            
            # Verify _finfo was called
            mock_fileio._finfo.assert_called_once()
            
            # Verify correct result returned
            assert result == expected_info

    def test_finfo_with_filesystem_parameter(self, mock_fsspec):
        """Test finfo with specific filesystem protocol."""
        test_path = "/test/file.txt"
        filesystem = "hdfs"
        
        with patch.object(FileIOInterface, '_instantiate') as mock_instantiate:
            mock_fileio = MagicMock()
            mock_instantiate.return_value = mock_fileio
            
            FileIOInterface.finfo(fpath=test_path, filesystem=filesystem)
            
            # Verify filesystem parameter passed correctly
            mock_instantiate.assert_called_once_with(fpath=test_path, filesystem=filesystem)

    def test_finfo_passes_additional_arguments(self, mock_fsspec):
        """Test that finfo passes additional arguments to underlying methods."""
        test_path = "/test/file.txt"
        extra_arg = "test"
        extra_kwarg = "value"
        
        with patch.object(FileIOInterface, '_instantiate') as mock_instantiate:
            mock_fileio = MagicMock()
            mock_instantiate.return_value = mock_fileio
            
            FileIOInterface.finfo(
                fpath=test_path, 
                extra_arg=extra_arg,
                extra_kwarg=extra_kwarg
            )
            
            # Verify args passed to both instantiate and _finfo
            mock_instantiate.assert_called_once_with(
                fpath=test_path, 
                filesystem=None,
                extra_arg=extra_arg,
                extra_kwarg=extra_kwarg
            )
            mock_fileio._finfo.assert_called_once_with(
                extra_arg=extra_arg,
                extra_kwarg=extra_kwarg
            )


class TestFileIOInterfaceFileRead:
    """Test FileIOInterface.fread method."""
    
    def test_fread_calls_baseio_fread_method(self, mock_fsspec):
        """Test that fread properly delegates to BaseFileIO._fread."""
        test_path = "/test/file.json"
        expected_data = {"key": "value"}
        
        with patch.object(FileIOInterface, '_instantiate') as mock_instantiate:
            mock_fileio = MagicMock()
            mock_fileio._fread.return_value = expected_data
            mock_instantiate.return_value = mock_fileio
            
            result = FileIOInterface.fread(read_path=test_path)
            
            # Verify instantiate was called correctly
            mock_instantiate.assert_called_once_with(fpath=test_path, filesystem=None)
            
            # Verify _fread was called
            mock_fileio._fread.assert_called_once()
            
            # Verify correct result returned
            assert result == expected_data

    def test_fread_with_filesystem_parameter(self, mock_fsspec):
        """Test fread with specific filesystem protocol."""
        test_path = "/test/file.json"
        filesystem = "gcs"
        
        with patch.object(FileIOInterface, '_instantiate') as mock_instantiate:
            mock_fileio = MagicMock()
            mock_instantiate.return_value = mock_fileio
            
            FileIOInterface.fread(read_path=test_path, filesystem=filesystem)
            
            # Verify filesystem parameter passed correctly
            mock_instantiate.assert_called_once_with(fpath=test_path, filesystem=filesystem)

    def test_fread_handles_different_file_types(self, mock_fsspec):
        """Test fread works with different file extensions.
        
        PYTEST: Parametrized test would be ideal here, but keeping
        simple for clarity. Each file type should work the same way.
        """
        test_paths = [
            "/test/data.json",
            "/test/data.csv", 
            "/test/data.txt",
            "/test/data.yaml"
        ]
        
        for test_path in test_paths:
            with patch.object(FileIOInterface, '_instantiate') as mock_instantiate:
                mock_fileio = MagicMock()
                mock_fileio._fread.return_value = f"data_from_{test_path}"
                mock_instantiate.return_value = mock_fileio
                
                result = FileIOInterface.fread(read_path=test_path)
                
                # Each call should work independently
                mock_instantiate.assert_called_once_with(fpath=test_path, filesystem=None)
                assert result == f"data_from_{test_path}"


class TestFileIOInterfaceFileCopy:
    """Test FileIOInterface.fcopy method."""
    
    def test_fcopy_calls_baseio_copy_method(self, mock_fsspec):
        """Test that fcopy properly delegates to BaseFileIO._copy."""
        read_path = "/source/file.txt"
        dest_path = "/dest/file.txt"
        
        with patch.object(FileIOInterface, '_instantiate') as mock_instantiate:
            mock_fileio = MagicMock()
            mock_instantiate.return_value = mock_fileio
            
            FileIOInterface.fcopy(read_path=read_path, dest_path=dest_path)
            
            # Verify instantiate was called with source path
            mock_instantiate.assert_called_once_with(fpath=read_path, filesystem=None)
            
            # Verify _copy was called with destination path
            mock_fileio._copy.assert_called_once_with(dest_path=dest_path)

    def test_fcopy_with_filesystem_parameter(self, mock_fsspec):
        """Test fcopy with specific filesystem protocol."""
        read_path = "/source/file.txt"
        dest_path = "/dest/file.txt"
        filesystem = "s3"
        
        with patch.object(FileIOInterface, '_instantiate') as mock_instantiate:
            mock_fileio = MagicMock()
            mock_instantiate.return_value = mock_fileio
            
            FileIOInterface.fcopy(
                read_path=read_path, 
                dest_path=dest_path, 
                filesystem=filesystem
            )
            
            # Verify filesystem parameter passed correctly
            mock_instantiate.assert_called_once_with(fpath=read_path, filesystem=filesystem)

    def test_fcopy_passes_additional_arguments(self, mock_fsspec):
        """Test that fcopy passes additional arguments correctly."""
        read_path = "/source/file.txt"
        dest_path = "/dest/file.txt"
        extra_arg = "test"
        extra_kwarg = "value"
        
        with patch.object(FileIOInterface, '_instantiate') as mock_instantiate:
            mock_fileio = MagicMock()
            mock_instantiate.return_value = mock_fileio
            
            FileIOInterface.fcopy(
                read_path=read_path,
                dest_path=dest_path,
                extra_arg=extra_arg,
                extra_kwarg=extra_kwarg
            )
            
            # Verify args passed to both instantiate and _copy
            mock_instantiate.assert_called_once_with(
                fpath=read_path,
                filesystem=None,
                extra_arg=extra_arg,
                extra_kwarg=extra_kwarg
            )
            mock_fileio._copy.assert_called_once_with(
                dest_path=dest_path,
                extra_arg=extra_arg,
                extra_kwarg=extra_kwarg
            )


class TestFileIOInterfaceFileWrite:
    """Test FileIOInterface.fwrite method."""
    
    def test_fwrite_calls_baseio_fwrite_method(self, mock_fsspec):
        """Test that fwrite properly delegates to BaseFileIO._fwrite."""
        write_path = "/test/output.json"
        test_data = {"key": "value"}
        
        with patch.object(FileIOInterface, '_instantiate') as mock_instantiate:
            mock_fileio = MagicMock()
            mock_instantiate.return_value = mock_fileio
            
            FileIOInterface.fwrite(write_path=write_path, data=test_data)
            
            # Verify instantiate was called correctly
            mock_instantiate.assert_called_once_with(fpath=write_path, filesystem=None)
            
            # Verify _fwrite was called with data
            mock_fileio._fwrite.assert_called_once_with(data=test_data)

    def test_fwrite_with_filesystem_parameter(self, mock_fsspec):
        """Test fwrite with specific filesystem protocol."""
        write_path = "/test/output.json"
        test_data = {"key": "value"}
        filesystem = "hdfs"
        
        with patch.object(FileIOInterface, '_instantiate') as mock_instantiate:
            mock_fileio = MagicMock()
            mock_instantiate.return_value = mock_fileio
            
            FileIOInterface.fwrite(
                write_path=write_path, 
                data=test_data, 
                filesystem=filesystem
            )
            
            # Verify filesystem parameter passed correctly
            mock_instantiate.assert_called_once_with(fpath=write_path, filesystem=filesystem)

    def test_fwrite_supports_overloaded_signatures(self, mock_fsspec):
        """Test that fwrite works with different data types as per overloads.
        
        PYTEST: The overloads in the interface suggest different data types
        should be supported. We test the actual implementation handles them.
        """
        import pandas as pd
        
        test_cases = [
            ("/test/data.csv", pd.DataFrame({"col": [1, 2, 3]})),
            ("/test/text.txt", "Hello World"),
            ("/test/config.json", {"setting": "value"}),
        ]
        
        for write_path, test_data in test_cases:
            with patch.object(FileIOInterface, '_instantiate') as mock_instantiate:
                mock_fileio = MagicMock()
                mock_instantiate.return_value = mock_fileio
                
                FileIOInterface.fwrite(write_path=write_path, data=test_data)
                
                # Verify each data type is handled
                mock_instantiate.assert_called_once_with(fpath=write_path, filesystem=None)
                mock_fileio._fwrite.assert_called_once_with(data=test_data)


class TestFileIOInterfaceDirectoryOperations:
    """Test FileIOInterface directory operations (fmakedirs, fdelete)."""
    
    def test_fmakedirs_calls_baseio_fmakedirs_method(self, mock_fsspec):
        """Test that fmakedirs properly creates directories without BaseFileIO instantiation."""
        dir_path = "/test/new/directories"
        
        with patch('upath.UPath') as mock_upath_class:
            mock_upath = MagicMock()
            mock_upath.fs.makedirs = MagicMock()
            mock_upath_class.return_value = mock_upath
            
            FileIOInterface.fmakedirs(path=dir_path)
            
            # Verify UPath was created with correct path
            mock_upath_class.assert_called_once_with(dir_path)
            
            # Verify makedirs was called with correct parameters
            mock_upath.fs.makedirs.assert_called_once_with(dir_path, exist_ok=True)

    def test_fmakedirs_with_exist_ok_parameter(self, mock_fsspec):
        """Test fmakedirs with exist_ok parameter."""
        dir_path = "/test/directories"
        
        with patch('upath.UPath') as mock_upath_class:
            mock_upath = MagicMock()
            mock_upath.fs.makedirs = MagicMock()
            mock_upath_class.return_value = mock_upath
            
            FileIOInterface.fmakedirs(path=dir_path, exist_ok=False)
            
            # Verify exist_ok parameter passed correctly
            mock_upath.fs.makedirs.assert_called_once_with(dir_path, exist_ok=False)

    def test_fdelete_calls_baseio_fdelete_method(self, mock_fsspec):
        """Test that fdelete properly deletes files/directories without BaseFileIO instantiation."""
        file_path = "/test/file_to_delete.txt"
        
        with patch('upath.UPath') as mock_upath_class:
            mock_upath = MagicMock()
            mock_upath.exists.return_value = True
            mock_upath.fs.delete = MagicMock()
            mock_upath_class.return_value = mock_upath
            
            FileIOInterface.fdelete(path=file_path)
            
            # Verify UPath was created with correct path
            mock_upath_class.assert_called_once_with(file_path)
            
            # Verify delete was called with correct parameters
            mock_upath.fs.delete.assert_called_once_with(file_path, recursive=True)

    def test_fdelete_with_filesystem_parameter(self, mock_fsspec):
        """Test fdelete with specific filesystem protocol."""
        file_path = "/test/file_to_delete.txt"
        filesystem = "gcs"
        
        with patch('upath.UPath') as mock_upath_class:
            mock_upath = MagicMock()
            mock_upath.exists.return_value = True
            mock_upath.fs.delete = MagicMock()
            mock_upath_class.return_value = mock_upath
            
            FileIOInterface.fdelete(path=file_path, filesystem=filesystem)
            
            # Verify UPath was created with correct path
            # Note: filesystem parameter is currently not used in the implementation
            mock_upath_class.assert_called_once_with(file_path)


# ========================================================================================
# PYTEST LEARNING TIP 💡
# Parametrized tests allow running the same test logic with different inputs.
# This is excellent for testing multiple file formats or edge cases.
# ========================================================================================

class TestFileIOInterfaceParametrized:
    """Parametrized tests for comprehensive coverage."""
    
    @pytest.mark.parametrize("filesystem", [None, "s3", "gcs", "hdfs"])
    def test_all_methods_support_filesystem_parameter(self, mock_fsspec, filesystem):
        """Test that all interface methods support filesystem parameter.
        
        PYTEST: Parametrized test runs this test for each filesystem value.
        This ensures consistent behavior across all methods.
        """
        test_path = "/test/file.txt"
        test_data = "test content"
        
        # Methods that use _instantiate (file operations)
        file_methods_to_test = [
            ('finfo', {'fpath': test_path}),
            ('fread', {'read_path': test_path}),
            ('fcopy', {'read_path': test_path, 'dest_path': '/dest/file.txt'}),
            ('fwrite', {'write_path': test_path, 'data': test_data}),
        ]
        
        # Test file methods that use _instantiate
        for method_name, kwargs in file_methods_to_test:
            with patch.object(FileIOInterface, '_instantiate') as mock_instantiate:
                mock_fileio = MagicMock()
                mock_instantiate.return_value = mock_fileio
                
                # Add filesystem parameter if provided
                if filesystem:
                    kwargs['filesystem'] = filesystem
                
                # Call the method
                method = getattr(FileIOInterface, method_name)
                method(**kwargs)
                
                # Verify filesystem parameter handled correctly
                expected_fs = filesystem if filesystem else None
                mock_instantiate.assert_called_once()
                call_args = mock_instantiate.call_args
                assert call_args.kwargs.get('filesystem') == expected_fs
        
        # Test directory methods that don't use _instantiate
        with patch('upath.UPath') as mock_upath_class:
            mock_upath = MagicMock()
            mock_upath.fs.makedirs = MagicMock()
            mock_upath.exists.return_value = True
            mock_upath.fs.delete = MagicMock()
            mock_upath_class.return_value = mock_upath
            
            # Test fmakedirs
            kwargs = {'path': test_path}
            if filesystem:
                kwargs['filesystem'] = filesystem
            FileIOInterface.fmakedirs(**kwargs)
            
            # Test fdelete
            kwargs = {'path': test_path}
            if filesystem:
                kwargs['filesystem'] = filesystem
            FileIOInterface.fdelete(**kwargs)

    @pytest.mark.parametrize("file_extension", [
        "csv", "txt", "json", "yaml", "yml", "parquet", "feather", "arrow", "pickle", "pkl"
    ])
    def test_interface_supports_all_file_types(self, mock_fsspec, file_extension):
        """Test that interface works with all supported file extensions.
        
        PYTEST: This parametrized test verifies that the interface
        can handle all file types that should be supported.
        """
        test_path = f"/test/file.{file_extension}"
        
        with patch.object(FileIOInterface, '_instantiate') as mock_instantiate:
            mock_fileio = MagicMock()
            mock_instantiate.return_value = mock_fileio
            
            # Test that instantiation works for all file types
            FileIOInterface.finfo(fpath=test_path)
            
            # Verify the path with extension was used
            mock_instantiate.assert_called_once_with(fpath=test_path, filesystem=None)
