"""
Comprehensive test suite for FileIOInterface class.

This file demonstrates pytest best practices following FIRST principles:
- Fast: Tests run quickly with minimal setup using mocks
- Independent: Each test can run in isolation using fixtures
- Repeatable: Tests produce consistent results using controlled mocks
- Self-Validating: Clear pass/fail with assertions
- Timely: Tests written alongside code
"""

import pytest
from unittest.mock import patch, MagicMock
import pandas as pd

from src.main.file_io import FileIOInterface

pytestmark = pytest.mark.unit


class TestFileIOInterfaceInstantiation:
    """Test FileIOInterface._instantiate method."""
    
    def test_instantiate_creates_base_fileio_object(self, mock_fsspec):
        """Test that _instantiate creates a BaseFileIO object correctly."""
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

    @pytest.mark.parametrize("filesystem", ["s3", "gcs", "hdfs", "file"])
    def test_instantiate_with_filesystem_parameter(self, mock_fsspec, filesystem):
        """Test _instantiate with different filesystem protocols."""
        test_path = "/test/file.txt"
        
        with patch('src.main.file_io.UPath') as mock_upath_class, \
             patch('src.main.file_io.BaseFileIO'):
            
            FileIOInterface._instantiate(fpath=test_path, filesystem=filesystem)
            
            # Verify UPath was created with filesystem protocol
            mock_upath_class.assert_called_once_with(test_path, protocol=filesystem)

    def test_instantiate_validates_filesystem_protocol(self, mock_fsspec):
        """Test that invalid filesystem protocols are rejected."""
        test_path = "/test/file.txt"
        invalid_filesystem = "invalid_protocol"
        
        # Test ValueError for invalid protocol
        with pytest.raises(ValueError, match="Unsupported filesystem"):
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

class TestFileIOInterfaceFileExists:
    """Test FileIOInterface.fexists method."""
    
    @pytest.mark.parametrize("expected_exists", [True, False])
    def test_fexists_calls_baseio_fexists_method(self, mock_fsspec, mock_instantiate, expected_exists):
        """Test that fexists properly delegates to BaseFileIO._fexists for files."""
        test_path = "/test/file.txt"
        
        mock_instantiate['fileio']._fexists.return_value = expected_exists

        result = FileIOInterface.fexists(test_path)

        # Verify _instantiate was called correctly (using keyword args)
        mock_instantiate['mock'].assert_called_once_with(fpath=test_path, filesystem=None)
        # Verify _fexists was called on the FileIO object
        mock_instantiate['fileio']._fexists.assert_called_once()
        assert result == expected_exists

    def test_fexists_with_filesystem_parameter(self, mock_fsspec, mock_instantiate):
        """Test fexists with specific filesystem protocol."""
        test_path = "/test/file.txt"
        filesystem = "s3"
        expected_exists = True
        
        mock_instantiate['fileio']._fexists.return_value = expected_exists

        result = FileIOInterface.fexists(test_path, filesystem=filesystem)
        
        # Verify _instantiate was called with filesystem parameter (keyword args)
        mock_instantiate['mock'].assert_called_once_with(fpath=test_path, filesystem=filesystem)
        mock_instantiate['fileio']._fexists.assert_called_once()
        assert result == expected_exists

class TestFileIOInterfaceFileInfo:
    """Test FileIOInterface.finfo method."""
    
    def test_finfo_calls_baseio_finfo_method(self, mock_fsspec, mock_instantiate):
        """Test that finfo properly delegates to BaseFileIO._finfo."""
        test_path = "/test/file.txt"
                
        result = FileIOInterface.finfo(fpath=test_path)
        
        # Verify instantiate was called correctly (using keyword args)
        mock_instantiate['mock'].assert_called_once_with(fpath=test_path, filesystem=None)
        
        # Verify _finfo was called
        mock_instantiate['fileio']._finfo.assert_called_once()
        
        # Verify correct result returned
        assert result == mock_instantiate['fileio']._finfo.return_value

    def test_finfo_passes_additional_arguments(self, mock_fsspec, mock_instantiate):
        """Test that finfo passes additional arguments to underlying methods."""
        test_path = "/test/file.txt"
        extra_arg = "test"
        extra_kwarg = "value"
        
        FileIOInterface.finfo(
            fpath=test_path, 
            extra_arg=extra_arg,
            extra_kwarg=extra_kwarg
        )
        
        # Verify args passed to both instantiate and _finfo
        mock_instantiate['mock'].assert_called_once_with(
            fpath=test_path, 
            filesystem=None,
            extra_arg=extra_arg,
            extra_kwarg=extra_kwarg
        )
        mock_instantiate['fileio']._finfo.assert_called_once_with(
            extra_arg=extra_arg,
            extra_kwarg=extra_kwarg
        )


class TestFileIOInterfaceFileRead:
    """Test FileIOInterface.fread method."""
    
    def test_fread_calls_baseio_fread_method(self, mock_fsspec, mock_instantiate):
        """Test that fread properly delegates to BaseFileIO._fread."""
        test_path = "/test/file.json"
        expected_data = {"key": "value"}
        
        mock_instantiate['fileio']._fread.return_value = expected_data
        
        result = FileIOInterface.fread(read_path=test_path)
        
        # Verify instantiate was called correctly (using keyword args)
        mock_instantiate['mock'].assert_called_once_with(fpath=test_path, filesystem=None)
        
        # Verify _fread was called
        mock_instantiate['fileio']._fread.assert_called_once()
        
        # Verify correct result returned
        assert result == expected_data

    def test_fread_with_filesystem_parameter(self, mock_fsspec, mock_instantiate):
        """Test fread with specific filesystem protocol."""
        test_path = "/test/file.json"
        filesystem = "hdfs"
        
        FileIOInterface.fread(read_path=test_path, filesystem=filesystem)
        
        # Verify filesystem parameter passed correctly (using keyword args)
        mock_instantiate['mock'].assert_called_once_with(fpath=test_path, filesystem=filesystem)

    @pytest.mark.parametrize("test_path",
                             ["/test/data.json",
                              "/test/data.csv", 
                              "/test/data.txt",
                              "/test/data.yaml"])
    def test_fread_handles_different_file_types(self, mock_fsspec, mock_instantiate, test_path):
        """Test fread works with different file extensions."""
        
        mock_instantiate['mock'].reset_mock()
        mock_instantiate['fileio']._fread.return_value = f"data_from_{test_path}"
        
        result = FileIOInterface.fread(read_path=test_path)
        
        # Each call should work independently
        mock_instantiate['mock'].assert_called_once_with(fpath=test_path, filesystem=None)
        assert result == f"data_from_{test_path}"


class TestFileIOInterfaceFileCopy:
    """Test FileIOInterface.fcopy method."""
    
    def test_fcopy_calls_baseio_copy_method(self, mock_fsspec, mock_instantiate):
        """Test that fcopy properly delegates to BaseFileIO._fcopy."""
        read_path = "/source/file.txt"
        dest_path = "/dest/file.txt"
        
        FileIOInterface.fcopy(read_path=read_path, dest_path=dest_path)
        
        # Verify instantiate was called with source path (using keyword args)
        mock_instantiate['mock'].assert_called_once_with(fpath=read_path, filesystem=None)
        
        # Verify _fcopy was called with destination path
        mock_instantiate['fileio']._fcopy.assert_called_once_with(dest_path=dest_path)

    def test_fcopy_with_filesystem_parameter(self, mock_fsspec, mock_instantiate):
        """Test fcopy with specific filesystem protocol."""
        read_path = "/source/file.txt"
        dest_path = "/dest/file.txt"
        filesystem = "hdfs"
        
        FileIOInterface.fcopy(
            read_path=read_path, 
            dest_path=dest_path, 
            filesystem=filesystem
        )
        
        # Verify filesystem parameter passed correctly (using keyword args)
        mock_instantiate['mock'].assert_called_once_with(fpath=read_path, filesystem=filesystem)

    def test_fcopy_passes_additional_arguments(self, mock_fsspec, mock_instantiate):
        """Test that fcopy passes additional arguments correctly."""
        read_path = "/source/file.txt"
        dest_path = "/dest/file.txt"
        extra_arg = "test"
        extra_kwarg = "value"
        
        FileIOInterface.fcopy(
            read_path=read_path,
            dest_path=dest_path,
            extra_arg=extra_arg,
            extra_kwarg=extra_kwarg
        )
        
        # Verify args passed to both instantiate and _fcopy
        mock_instantiate['mock'].assert_called_once_with(
            fpath=read_path,
            filesystem=None,
            extra_arg=extra_arg,
            extra_kwarg=extra_kwarg
        )
        mock_instantiate['fileio']._fcopy.assert_called_once_with(
            dest_path=dest_path,
            extra_arg=extra_arg,
            extra_kwarg=extra_kwarg
        )


class TestFileIOInterfaceFileWrite:
    """Test FileIOInterface.fwrite method."""
    
    def test_fwrite_calls_baseio_fwrite_method(self, mock_fsspec, mock_instantiate):
        """Test that fwrite properly delegates to BaseFileIO._fwrite."""
        write_path = "/test/output.json"
        test_data = {"key": "value"}
        
        FileIOInterface.fwrite(write_path=write_path, data=test_data)
        
        # Verify instantiate was called correctly (using keyword args)
        mock_instantiate['mock'].assert_called_once_with(fpath=write_path, filesystem=None)
        
        # Verify _fwrite was called with data
        mock_instantiate['fileio']._fwrite.assert_called_once_with(data=test_data)

    def test_fwrite_with_filesystem_parameter(self, mock_fsspec, mock_instantiate):
        """Test fwrite with specific filesystem protocol."""
        write_path = "/test/output.json"
        test_data = {"key": "value"}
        filesystem = "hdfs"
        
        FileIOInterface.fwrite(
            write_path=write_path, 
            data=test_data, 
            filesystem=filesystem
        )
        
        # Verify filesystem parameter passed correctly (using keyword args)
        mock_instantiate['mock'].assert_called_once_with(fpath=write_path, filesystem=filesystem)

    @pytest.mark.parametrize("write_path, test_data", [
        ("/test/data.csv", pd.DataFrame({"col": [1, 2, 3]})),
        ("/test/text.txt", "Hello World"),
        ("/test/config.json", {"setting": "value"}),
    ])
    def test_fwrite_supports_overloaded_signatures(self, mock_fsspec, mock_instantiate, write_path, test_data):
        """Test that fwrite works with different data types as per overloads."""        
    
        # Reset the mock for each iteration
        mock_instantiate['mock'].reset_mock()
        
        FileIOInterface.fwrite(write_path=write_path, data=test_data)
        
        # Verify each data type is handled
        mock_instantiate['mock'].assert_called_once_with(fpath=write_path, filesystem=None)
        mock_instantiate['fileio']._fwrite.assert_called_once_with(data=test_data)


class TestFileIOInterfaceDirectoryOperations:
    """Test FileIOInterface directory operations (fmakedirs, fdelete)."""
    
    @pytest.mark.parametrize("exist_ok, expected_call", [
        (True, True),
        (False, False),
    ])
    def test_fmakedirs_with_exist_ok_parameter(self, mock_fsspec, exist_ok, expected_call):
        """Test fmakedirs with exist_ok parameter only (combined test)."""
        dir_path = "/test/directories"
        with patch('src.main.file_io.UPath') as mock_upath_class:
            mock_upath = MagicMock()
            mock_upath.fs.makedirs = MagicMock()
            mock_upath_class.return_value = mock_upath

            FileIOInterface.fmakedirs(path=dir_path, exist_ok=exist_ok)

            # If exist_ok is None, fmakedirs should default to exist_ok=True
            expected_exist_ok = expected_call
            mock_upath.fs.makedirs.assert_called_once_with(dir_path, exist_ok=expected_exist_ok)

    def test_fmakedirs_with_filesystem_parameter(self, mock_fsspec):
        """Test fmakedirs with specific filesystem protocol."""
        dir_path = "/test/directories"
        filesystem = "s3"
        
        with patch('src.main.file_io.UPath') as mock_upath_class:
            mock_upath = MagicMock()
            mock_upath.fs.makedirs = MagicMock()
            mock_upath_class.return_value = mock_upath

            FileIOInterface.fmakedirs(path=dir_path, filesystem=filesystem)

            # Verify UPath was created with correct filesystem protocol
            mock_upath_class.assert_called_once_with(dir_path, protocol=filesystem)
            
            # Verify makedirs was called with correct parameters
            mock_upath.fs.makedirs.assert_called_once_with(dir_path, exist_ok=True)

    def test_fmakedirs_validates_unsupported_filesystem_protocol(self, mock_fsspec):
        """Test that fmakedirs raises ValueError for unsupported filesystem protocols."""
        dir_path = "/test/directories"
        invalid_filesystem = "invalid_protocol"
        
        # Modify mock to not include the invalid filesystem
        mock_fsspec.return_value = ['file', 's3', 'gcs', 'hdfs']  # Exclude invalid_protocol
        
        # Test ValueError for invalid protocol
        with pytest.raises(ValueError, match="Unsupported filesystem"):
            FileIOInterface.fmakedirs(path=dir_path, filesystem=invalid_filesystem)

    def test_fmakedirs_handles_directory_creation_failure(self, mock_fsspec):
        """Test that fmakedirs properly handles OSError when directory creation fails."""
        dir_path = "/test/failing_directory"
        
        with patch('src.main.file_io.UPath') as mock_upath_class, \
             patch('warnings.warn') as mock_warn:
            
            mock_upath = MagicMock()
            # Configure makedirs to raise OSError
            test_error = OSError("Permission denied")
            mock_upath.fs.makedirs.side_effect = test_error
            mock_upath_class.return_value = mock_upath

            # Test that OSError is re-raised after warning
            with pytest.raises(OSError, match="Permission denied"):
                FileIOInterface.fmakedirs(path=dir_path)

            # Verify warning was issued
            mock_warn.assert_called_with(f"Failed to create directories for {dir_path}: {test_error}")
            
            # Verify UPath was created correctly
            mock_upath_class.assert_called_with(dir_path, protocol=None)
            
            # Verify makedirs was attempted
            mock_upath.fs.makedirs.assert_called_with(dir_path, exist_ok=True)

    def test_fdelete_calls_baseio_fdelete_method(self, mock_fsspec, mock_instantiate):
        """Test that fdelete properly deletes files using BaseFileIO for files."""
        file_path = "/test/file_to_delete.txt"
        
        with patch('src.main.file_io.UPath') as mock_upath_class:
            mock_upath = MagicMock()
            mock_upath.is_dir.return_value = False  # It's a file, not directory
            mock_upath_class.return_value = mock_upath
            
            FileIOInterface.fdelete(path=file_path)
            
            # Verify UPath was created with correct path
            mock_upath_class.assert_called_once_with(file_path, protocol=None)
            
            # Verify BaseFileIO instantiate was called (using keyword args)
            mock_instantiate['mock'].assert_called_once_with(fpath=file_path, filesystem=None)
            
            # Verify _fdelete was called with correct parameters
            mock_instantiate['fileio']._fdelete.assert_called_once_with(filepath=file_path)

    def test_fdelete_with_filesystem_parameter(self, mock_fsspec):
        """Test fdelete directory deletion with specific filesystem protocol."""
        dir_path = "/test/directory_to_delete"
        filesystem = "gcs"
        
        with patch('src.main.file_io.UPath') as mock_upath_class:
            mock_upath = MagicMock()
            mock_upath.is_dir.return_value = True  # It's a directory
            mock_upath.path = dir_path
            mock_upath.fs.rm = MagicMock()
            mock_upath_class.return_value = mock_upath
            
            FileIOInterface.fdelete(path=dir_path, filesystem=filesystem)
            
            # Verify UPath was created with correct path and protocol
            mock_upath_class.assert_called_once_with(dir_path, protocol=filesystem)
            
            # Verify rm was called with recursive=True for directory
            mock_upath.fs.rm.assert_called_once_with(dir_path, recursive=True)

class TestFileIOInterfaceParametrized:
    """Parametrized tests for comprehensive coverage."""
    
    @pytest.mark.parametrize("filesystem", [None, "s3", "gcs", "hdfs"])
    def test_all_methods_support_filesystem_parameter(self, mock_fsspec, filesystem, mock_instantiate):
        """Test that all interface methods support filesystem parameter."""
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
        
            # Add filesystem parameter if provided
            if filesystem:
                kwargs['filesystem'] = filesystem
            
            # Call the method
            method = getattr(FileIOInterface, method_name)
            method(**kwargs)
            
            # Verify filesystem parameter handled correctly
            expected_fs = filesystem if filesystem else None
            mock_instantiate["mock"].assert_called()
            call_args = mock_instantiate["mock"].call_args
            assert call_args.kwargs.get('filesystem') == expected_fs
        
        # Test directory methods that don't use _instantiate
        with patch('src.main.file_io.UPath') as mock_upath_class:
            mock_upath = MagicMock()
            mock_upath.fs.makedirs = MagicMock()
            mock_upath.is_dir.return_value = True  # For fdelete directory test
            mock_upath.path = test_path
            mock_upath.fs.rm = MagicMock()
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
        # Text-based formats (require string data)
        "txt", "text", "log", "logs", "sql",
        # JSON/YAML formats (require serializable data)  
        "json", "yaml", "yml",
        # DataFrame formats (require pandas DataFrame)
        "csv", "parquet", "arrow", "feather",
        # Pickle formats (can handle any serializable data)
        "pickle", "pkl"
    ])
    def test_interface_supports_all_file_types(self, mock_fsspec, mock_instantiate, file_extension):
        """Test that interface works with all supported file extensions."""
        test_path = f"/test/file.{file_extension}"
        
        # Test that instantiation works for all file types
        FileIOInterface.finfo(fpath=test_path)
        
        # Verify the path with extension was used (using keyword args)
        mock_instantiate['mock'].assert_called_once_with(fpath=test_path, filesystem=None)
