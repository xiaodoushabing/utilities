"""
Comprehensive test suite for BaseFileIO class.

This file tests the core BaseFileIO functionality that underlies
the FileIOInterface. Tests cover file validation, read/write operations,
directory management, and error handling.
"""

import pytest
import warnings
from io import BytesIO
from unittest.mock import patch, MagicMock

from src.main.file_io._base import BaseFileIO
from src.main.file_io._base import fileio_mapping
from src.main.file_io.json import JsonFileIO
from src.main.file_io.csv import CSVFileIO
from src.main.file_io.text import TextFileIO

pytestmark = pytest.mark.unit

class TestBaseFileIOInitialization:
    """Test BaseFileIO initialization and validation."""
    
    def test_basefileio_initialization_with_valid_extension(self, mock_upath):
        """Test BaseFileIO initializes correctly with valid file extension."""
        
        with patch.object(BaseFileIO, '_validate_file_extension', return_value="txt"):
            fileio = BaseFileIO(upath_obj=mock_upath)
            
            assert fileio.upath == mock_upath
            assert fileio.file_extension == "txt"  # From mock_upath.suffix

    def test_basefileio_validates_extension_during_init(self, mock_upath):
        """Test that file extension validation is called during initialization."""
        
        with patch.object(BaseFileIO, '_validate_file_extension') as mock_validate:
            BaseFileIO(upath_obj=mock_upath)
            mock_validate.assert_called_once()

    @pytest.mark.parametrize("extension,expected", [
        ("json", "json"),
        ("txt", "txt"), 
        ("csv", "csv"),
        ("yaml", "yaml"),
        ("yml", "yml"),
        ("parquet", "parquet"),
        ("pkl", "pkl"),
        ("pickle", "pickle")
    ])
    def test_validate_file_extension_with_supported_formats(self, extension, expected):
        """Test _validate_file_extension with all supported formats."""
        mock_upath = MagicMock()
        mock_upath.suffix = f".{extension}"
        mock_upath.path = f"test.{extension}"
        
        fileio = BaseFileIO(upath_obj=mock_upath)
        assert fileio.file_extension == expected

    @pytest.mark.parametrize("invalid_input,expected_error", [
        ("", "has no extension"),
        (".xyz", "Unsupported file format"),
        (".unknown", "Unsupported file format")
    ])
    def test_validate_file_extension_rejects_invalid_formats(self, invalid_input, expected_error):
        """Test that invalid file formats are properly rejected."""
        mock_upath = MagicMock()
        mock_upath.suffix = invalid_input
        mock_upath.path = f"test{invalid_input}" if invalid_input else "test_file_no_extension"
        
        with pytest.raises(ValueError, match=expected_error):
            BaseFileIO(upath_obj=mock_upath)

    @pytest.mark.parametrize("case_variant,expected", [
        (".JSON", "json"),
        (".Txt", "txt"),
        (".CSV", "csv"),
        (".YAML", "yaml")
    ])
    def test_validate_file_extension_case_insensitive(self, case_variant, expected):
        """Test that file extension validation is case-insensitive."""
        mock_upath = MagicMock()
        mock_upath.suffix = case_variant
        mock_upath.path = f"test{case_variant}"
        
        fileio = BaseFileIO(upath_obj=mock_upath)
        assert fileio.file_extension == expected


class TestBaseFileIOFileInfo:
    """Test BaseFileIO._finfo method."""
    
    def test_finfo_returns_file_information(self, mock_upath):
        """Test that _finfo returns file system information."""
        expected_info = {"size": 123, "type": "file", "mtime": 1234567890}
        mock_upath.fs.info.return_value = expected_info
        
        fileio = BaseFileIO(upath_obj=mock_upath)
        result = fileio._finfo()
        
        mock_upath.fs.info.assert_called_once_with(mock_upath.path)
        assert result == expected_info

    def test_finfo_passes_additional_arguments(self, mock_upath):
        """Test that _finfo passes additional arguments to fs.info."""
        fileio = BaseFileIO(upath_obj=mock_upath)
        fileio._finfo("extra_arg", detail="full")
        
        mock_upath.fs.info.assert_called_once_with(
            mock_upath.path, "extra_arg", detail="full"
        )

    def test_finfo_handles_os_error_with_warning(self, mock_upath):
        """Test that _finfo handles OSError and raises with warning."""
        mock_upath.fs.info.side_effect = OSError("File not found")
        
        fileio = BaseFileIO(upath_obj=mock_upath)
        
        with pytest.raises(OSError):
            with warnings.catch_warnings(record=True) as w:
                fileio._finfo()
                
                # Verify warning was issued
                assert len(w) == 1
                assert "does not exist or is not accessible" in str(w[0].message)


class TestBaseFileIOFileRead:
    """Test BaseFileIO._fread method."""
    
    def test_fread_checks_file_exists(self, mock_upath):
        """Test that _fread checks if file exists before reading."""
        mock_upath.exists.return_value = False
        
        fileio = BaseFileIO(upath_obj=mock_upath)

        with pytest.raises(FileNotFoundError, match=f"File not found: {mock_upath.path}"):
            fileio._fread()

    def test_fread_opens_file_in_binary_mode(self, mock_upath, mock_fileio_mapping, mock_file_context):
        """Test that _fread opens file in binary mode and passes content to format parser."""
       
        # Configure the mock file IO class
        mock_fileio_mapping._read.return_value = {"key": "value"}

        fileio = BaseFileIO(upath_obj=mock_upath)
        result = fileio._fread()
        
        # Verify file opened in binary mode
        mock_upath.fs.open.assert_called_once_with(mock_upath.path, 'rb')
        
        # Verify file content is read
        mock_file_context.read.assert_called_once_with()
        
        # Verify the raw content is passed to the format-specific parser
        # The _read method should be called with a BytesIO containing the file content
        mock_fileio_mapping._read.assert_called_once()
        args, _ = mock_fileio_mapping._read.call_args
        assert isinstance(args[0], BytesIO)
        # Verify the BytesIO contains the correct content
        args[0].seek(0)  # Reset position to read from beginning
        assert args[0].read() == mock_file_context.read.return_value
        
        assert result == {"key": "value"}

    @pytest.mark.parametrize("extension", ["json", "csv", "logs", "log", "txt", "text", "yaml", "yml", "pickle", "pkl", "parquet", "feather", "arrow"])
    def test_fread_uses_correct_fileio_class(self, mock_upath, extension, mock_fileio_mapping, mock_file_context):
        """Test that _fread uses the correct file IO class based on extension."""
        file_content = b"test content"
        mock_upath.suffix = f".{extension}"
        
        mock_file_context.read.return_value = file_content
        
        # Configure the mock file IO class (provided by mock_fileio_mapping fixture)
        mock_fileio_mapping._read.return_value = f"parsed_{extension}_data"
        
        fileio = BaseFileIO(upath_obj=mock_upath)
        result = fileio._fread()
        
        # Verify the file IO class _read method was called
        mock_fileio_mapping._read.assert_called_once()
        
        # Verify the correct content was passed to the parser
        args, _ = mock_fileio_mapping._read.call_args
        assert isinstance(args[0], BytesIO)
        args[0].seek(0)
        assert args[0].read() == file_content
        
        assert result == f"parsed_{extension}_data"

    @pytest.mark.parametrize("extension, expected_class", [
        ("json", JsonFileIO),
        ("csv", CSVFileIO),
        ("txt", TextFileIO),
    ])
    def test_fread_selects_actual_fileio_classes(self, mock_upath, extension, expected_class):
        """Test that _fread selects the actual FileIO classes from the real mapping."""
        mock_upath.suffix = f".{extension}"

        # Verify the mapping contains the expected class
        assert fileio_mapping[extension] == expected_class
        
        # Test that BaseFileIO uses this mapping correctly
        fileio = BaseFileIO(upath_obj=mock_upath)
        assert fileio.file_extension == extension

    def test_fread_with_offset_seeks_to_correct_position(self, mock_upath, mock_fileio_mapping, mock_file_context):
        """Test that _fread seeks to the correct offset position."""
        offset = 5
        
        fileio = BaseFileIO(upath_obj=mock_upath)
        result = fileio._fread(offset=offset)
        
        # Verify seek was called with correct offset
        mock_file_context.seek.assert_called_once_with(offset)
        # Verify read was called without size (reads to end of file)
        mock_file_context.read.assert_called_once_with()

    def test_fread_with_size_reads_specific_byte_count(self, mock_upath, mock_fileio_mapping, mock_file_context):
        """Test that _fread reads only the specified number of bytes."""
        expected_size = 10
        
        # Configure the mock file IO class
        mock_fileio_mapping._read.return_value = "parsed_10_bytes"
        
        fileio = BaseFileIO(upath_obj=mock_upath)
        result = fileio._fread(size=expected_size)
        
        # Verify seek was called with default offset (0)
        mock_file_context.seek.assert_called_once_with(0)
        # Verify read was called with specific size
        mock_file_context.read.assert_called_once_with(expected_size)
        assert result == "parsed_10_bytes"

    def test_fread_with_raw_bytes_returns_bytes_directly(self, mock_upath, mock_file_context):
        """Test that _fread returns raw bytes when raw_bytes=True, bypassing format parsing."""

        fileio = BaseFileIO(upath_obj=mock_upath)
        result = fileio._fread(raw_bytes=True)
        
        # Verify file was read
        mock_file_context.seek.assert_called_once_with(0)
        mock_file_context.read.assert_called_once_with()
        
        # Verify raw bytes returned without format parsing
        assert result == mock_file_context.read.return_value
        assert isinstance(result, bytes)

    def test_fread_raw_bytes_with_offset_and_size(self, mock_upath, mock_file_context):
        """Test that _fread raw_bytes mode works correctly with offset and size."""
        mock_upath.exists.return_value = True
        offset = 3
        size = 5
        expected_bytes = b"bytes"
        
        # Reset the mock file content for this specific test
        mock_file_context.read.return_value = expected_bytes
        
        fileio = BaseFileIO(upath_obj=mock_upath)
        result = fileio._fread(offset=offset, size=size, raw_bytes=True)
        
        # Verify correct seek and read operations
        mock_file_context.seek.assert_called_once_with(offset)
        mock_file_context.read.assert_called_once_with(size)
        
        # Verify raw bytes returned
        assert result == expected_bytes
        assert isinstance(result, bytes)

    @pytest.mark.parametrize("offset,size", [
        (0, None),      # Read from start, entire file
        (10, None),     # Read from offset, entire remainder
        (0, 20),        # Read from start, specific size
        (5, 15),        # Read from offset, specific size
        (100, 50),      # Large offset and size
    ])
    def test_fread_offset_size_parameter_combinations(self, mock_upath, offset, size, mock_fileio_mapping, mock_file_context):
        """Test various combinations of offset and size parameters."""
        mock_upath.exists.return_value = True
        
        mock_file_context.read.return_value = b"test content"
        
        # Configure the mock file IO class
        mock_fileio_mapping._read.return_value = "parsed_content"
        
        fileio = BaseFileIO(upath_obj=mock_upath)
        result = fileio._fread(offset=offset, size=size)
        
        # Verify seek was called with correct offset
        mock_file_context.seek.assert_called_once_with(offset)
        
        # Verify read was called correctly based on size parameter
        if size is not None:
            mock_file_context.read.assert_called_once_with(size)
        else:
            mock_file_context.read.assert_called_once_with()
        
        assert result == "parsed_content"


class TestBaseFileIOFileCopy:
    """Test BaseFileIO._fcopy method."""
    
    def test_copy_checks_source_file_exists(self, mock_upath):
        """Test that _fcopy checks if source file exists."""
        mock_upath.exists.return_value = False
        
        fileio = BaseFileIO(upath_obj=mock_upath)
        
        with pytest.raises(FileNotFoundError, match="Source file not found"):
            fileio._fcopy(dest_path="/dest/file.txt")

    @pytest.mark.parametrize("invalid_dest", ["", "   ", None])
    def test_copy_validates_destination_path(self, mock_upath, invalid_dest):
        """Test that _fcopy validates destination path is not empty."""
        mock_upath.suffix = ".txt"
        mock_upath.exists.return_value = True
        
        fileio = BaseFileIO(upath_obj=mock_upath)
        
        with pytest.raises(ValueError, match="Destination path cannot be empty"):
            fileio._fcopy(dest_path=invalid_dest)

    def test_copy_performs_file_copy_operation(self, mock_upath):
        """Test that _fcopy performs the actual file copying."""
        # Mock source file content
        source_content = b"test file content"
        
        # Create proper context manager mocks
        mock_src_file = MagicMock()
        mock_src_file.read.return_value = source_content
        mock_src_file.__enter__ = MagicMock(return_value=mock_src_file)
        mock_src_file.__exit__ = MagicMock(return_value=None)
        
        mock_dest_file = MagicMock()
        mock_dest_file.__enter__ = MagicMock(return_value=mock_dest_file)
        mock_dest_file.__exit__ = MagicMock(return_value=None)
        
        # Mock file system operations
        mock_upath.fs.open.return_value = mock_src_file
        
        # Mock destination UPath creation
        with patch('src.main.file_io._base.UPath') as mock_upath_class:
            mock_dest_upath = MagicMock()
            mock_dest_upath.path = "/dest/file.txt"
            mock_dest_fs = MagicMock()
            mock_dest_fs.open.return_value = mock_dest_file
            mock_dest_upath.fs = mock_dest_fs
            mock_upath_class.return_value = mock_dest_upath
            
            fileio = BaseFileIO(upath_obj=mock_upath)
            fileio._fcopy(dest_path="/dest/file.txt")
            
            # Verify source file was opened for reading
            mock_upath.fs.open.assert_called_with(mock_upath.path, 'rb')
            
            # Verify destination file was opened for writing
            mock_dest_fs.open.assert_called_with("/dest/file.txt", 'wb')
            
            # Verify content was written to destination
            mock_dest_file.write.assert_called_once_with(source_content)

    def test_copy_handles_os_error_with_warning(self, mock_upath):
        """Test that _fcopy handles OSError and raises with warning."""
        # Mock OS error during file operations
        mock_upath.fs.open.side_effect = OSError("Permission denied")
        
        fileio = BaseFileIO(upath_obj=mock_upath)
        
        with pytest.raises(OSError):
            with warnings.catch_warnings(record=True) as w:
                fileio._fcopy(dest_path="/dest/file.txt")
                
                # Verify warning was issued
                assert len(w) == 1
                assert "Failed to copy" in str(w[0].message)


class TestBaseFileIOFileWrite:
    """Test BaseFileIO._fwrite method."""
    
    def test_fwrite_validates_data_type_before_writing(self, mock_upath, mock_fileio_mapping):
        """Test that _fwrite validates data type before attempting write."""
        mock_upath.suffix = ".csv"
        
        fileio = BaseFileIO(upath_obj=mock_upath)
        
        with patch.object(fileio, '_validate_data_type') as mock_validate:
            test_data = "invalid data for csv"
            fileio._fwrite(data=test_data)
            
            # Verify validation was called with correct parameters
            mock_validate.assert_called_once_with(test_data, "csv")

    @pytest.mark.parametrize("extension,data_type", [
        ("json", {"key": "value"}),
        ("txt", "text content"),
        ("yaml", {"yaml": "data"})
    ])
    def test_fwrite_uses_correct_fileio_class(self, mock_upath, extension, data_type, mock_fileio_mapping):
        """Test that _fwrite uses correct file IO class based on extension."""
        mock_upath.suffix = f".{extension}"
        
        fileio = BaseFileIO(upath_obj=mock_upath)
        fileio._fwrite(data=data_type)
        
        # Verify write method was called with correct parameters (including mode)
        expected_mode = 'w' if extension in ['txt', 'json', 'yaml', 'csv'] else 'wb'
        mock_fileio_mapping._write.assert_called_once_with(mock_upath, data_type, mode=expected_mode)

    def test_fwrite_uses_correct_fileio_class_for_dataframe_formats(self, mock_upath, sample_dataframe, mock_fileio_mapping):
        """Test that _fwrite uses correct file IO class for DataFrame-requiring formats."""
        extension = "csv"
        mock_upath.suffix = f".{extension}"
        
        fileio = BaseFileIO(upath_obj=mock_upath)
        fileio._fwrite(data=sample_dataframe)
        
        # Verify write method was called with correct parameters
        mock_fileio_mapping._write.assert_called_once_with(mock_upath, sample_dataframe, mode='w')

    def test_fwrite_passes_additional_arguments(self, mock_upath, mock_fileio_mapping):
        """Test that _fwrite passes additional arguments to file IO class."""
        mock_upath.suffix = ".txt"
        
        fileio = BaseFileIO(upath_obj=mock_upath)
        fileio._fwrite(data="test", mode="w", encoding="utf-8")
        
        # Verify write method was called with additional arguments
        mock_fileio_mapping._write.assert_called_once_with(
            mock_upath, "test", mode="w", encoding="utf-8"
        )


class TestBaseFileIODataValidation:
    """Test BaseFileIO._validate_data_type method."""
    
    @pytest.mark.parametrize("file_extension", ["csv", "feather", "parquet", "arrow"])
    def test_validate_all_dataframe_formats(self, mock_upath, file_extension):
        """Test validation for all DataFrame-based formats."""
        import pandas as pd
        
        mock_upath.suffix = f".{file_extension}"
        fileio = BaseFileIO(upath_obj=mock_upath)
        
        # Valid DataFrame should pass
        valid_df = pd.DataFrame({"col": [1, 2, 3]})
        fileio._validate_data_type(valid_df, file_extension)  # Should not raise
        
        # Invalid data should raise TypeError
        with pytest.raises(TypeError, match=f"requires a pandas DataFrame"):
            fileio._validate_data_type({"not": "dataframe"}, file_extension)

    @pytest.mark.parametrize("file_extension", ["txt", "text", "log", "logs", "sql"])
    def test_validate_string_formats_require_string(self, mock_upath, file_extension):
        """Test that string formats require string data."""
        mock_upath.suffix = f".{file_extension}"
        fileio = BaseFileIO(upath_obj=mock_upath)
        
        # Valid string should pass
        fileio._validate_data_type("valid string", file_extension)  # Should not raise
        
        # Non-string should raise TypeError
        with pytest.raises(TypeError, match=f"requires a string"):
            fileio._validate_data_type({"not": "string"}, file_extension)

    @pytest.mark.parametrize("file_extension", ["json", "yaml", "yml", "pickle", "pkl"])
    def test_validate_serializable_formats_accept_any_data(self, mock_upath, file_extension):
        """Test that serializable formats accept various data types."""
        mock_upath.suffix = f".{file_extension}"
        fileio = BaseFileIO(upath_obj=mock_upath)
        
        # Various data types should all pass (no exceptions raised)
        test_data = [
            {"dict": "data"},
            ["list", "data"],
            "string data",
            123,
            True,
            None
        ]
        
        for data in test_data:
            fileio._validate_data_type(data, file_extension)  # Should not raise


class TestBaseFileIODirectoryOperations:
    """Test BaseFileIO directory operations."""
    
    @pytest.mark.parametrize("filepath", ["", "   ", None])
    def test_fdelete_validates_filepath_not_empty(self, mock_upath, filepath):
        """Test that _fdelete validates filepath is not empty."""
        fileio = BaseFileIO(upath_obj=mock_upath)
        
        # Test empty string
        with pytest.raises(ValueError, match="File path cannot be empty"):
            fileio._fdelete(filepath=filepath)


    def test_fdelete_warns_for_nonexistent_files(self, mock_upath):
        """Test that _fdelete warns but doesn't error for non-existent files."""
        fileio = BaseFileIO(upath_obj=mock_upath)
        
        with patch('src.main.file_io._base.UPath') as mock_upath_class:
            mock_delete_path = MagicMock()
            mock_delete_path.exists.return_value = False
            mock_upath_class.return_value = mock_delete_path
            
            with warnings.catch_warnings(record=True) as w:
                fileio._fdelete(filepath="/nonexistent/file.txt")
                
                # Verify warning was issued
                assert len(w) == 1
                assert "Path does not exist" in str(w[0].message)
                
                # Verify filesystem rm was NOT called
                mock_delete_path.fs.rm.assert_not_called()

    def test_fdelete_calls_filesystem_delete_for_existing_files(self, mock_upath):
        """Test that _fdelete calls filesystem rm for existing files."""
        fileio = BaseFileIO(upath_obj=mock_upath)
        test_path = "/test/file.txt"
        
        with patch('src.main.file_io._base.UPath') as mock_upath_class:
            mock_delete_path = MagicMock()
            mock_delete_path.exists.return_value = True
            mock_delete_path.path = test_path
            mock_delete_path.fs = MagicMock()
            mock_upath_class.return_value = mock_delete_path
            
            fileio._fdelete(filepath=test_path)
            
            # Verify filesystem rm was called
            mock_delete_path.fs.rm.assert_called_once_with(test_path)

    def test_fdelete_handles_os_error_with_warning(self, mock_upath):
        """Test that _fdelete handles OSError and raises with warning."""
        fileio = BaseFileIO(upath_obj=mock_upath)
        
        with patch('src.main.file_io._base.UPath') as mock_upath_class:
            mock_delete_path = MagicMock()
            mock_delete_path.exists.return_value = True
            mock_delete_path.fs.rm.side_effect = OSError("Permission denied")
            mock_upath_class.return_value = mock_delete_path
            
            with pytest.raises(OSError):
                with warnings.catch_warnings(record=True) as w:
                    fileio._fdelete(filepath="/test/file.txt")
                    
                    # Verify warning was issued
                    assert len(w) == 1
                    assert "Failed to delete" in str(w[0].message)
