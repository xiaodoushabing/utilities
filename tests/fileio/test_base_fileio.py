"""
Comprehensive test suite for BaseFileIO class.

This file tests the core BaseFileIO functionality that underlies
the FileIOInterface. Tests cover file validation, read/write operations,
directory management, and error handling.

Key testing areas:
📁 File extension validation
📖 File reading operations
✍️ File writing operations  
📋 File copying operations
📂 Directory operations
🚫 Error handling and edge cases
"""

import pytest
import warnings
from unittest.mock import patch, MagicMock, mock_open, call
from io import BytesIO

from src.main.file_io._base import BaseFileIO, fileio_mapping

pytestmark = pytest.mark.unit


class TestBaseFileIOInitialization:
    """Test BaseFileIO initialization and validation."""
    
    def test_basefileio_initialization_with_valid_extension(self, mock_upath):
        """Test BaseFileIO initializes correctly with valid file extension."""
        mock_upath.suffix = ".json"
        
        with patch.object(BaseFileIO, '_validate_file_extension', return_value="json"):
            fileio = BaseFileIO(upath_obj=mock_upath)
            
            assert fileio.upath == mock_upath
            assert fileio.file_extension == "json"

    def test_basefileio_validates_extension_during_init(self, mock_upath):
        """Test that file extension validation is called during initialization."""
        mock_upath.suffix = ".txt"
        
        with patch.object(BaseFileIO, '_validate_file_extension') as mock_validate:
            mock_validate.return_value = "txt"
            
            BaseFileIO(upath_obj=mock_upath)
            
            mock_validate.assert_called_once()

    def test_validate_file_extension_with_supported_formats(self):
        """Test _validate_file_extension with all supported formats."""
        supported_extensions = list(fileio_mapping.keys())
        
        for ext in supported_extensions:
            mock_upath = MagicMock()
            mock_upath.suffix = f".{ext}"
            mock_upath.path = f"test.{ext}"
            
            fileio = BaseFileIO(upath_obj=mock_upath)
            assert fileio.file_extension == ext

    def test_validate_file_extension_rejects_no_extension(self):
        """Test that files without extensions are rejected."""
        mock_upath = MagicMock()
        mock_upath.suffix = ""
        mock_upath.path = "test_file_no_extension"
        
        with pytest.raises(ValueError, match="has no extension"):
            BaseFileIO(upath_obj=mock_upath)

    def test_validate_file_extension_rejects_unsupported_format(self):
        """Test that unsupported file formats are rejected."""
        mock_upath = MagicMock()
        mock_upath.suffix = ".xyz"
        mock_upath.path = "test.xyz"
        
        with pytest.raises(ValueError, match="Unsupported file format"):
            BaseFileIO(upath_obj=mock_upath)

    def test_validate_file_extension_case_insensitive(self):
        """Test that file extension validation is case-insensitive."""
        test_cases = [".JSON", ".Txt", ".CSV", ".YAML"]
        expected = ["json", "txt", "csv", "yaml"]
        
        for ext, expected_ext in zip(test_cases, expected):
            mock_upath = MagicMock()
            mock_upath.suffix = ext
            mock_upath.path = f"test{ext}"
            
            fileio = BaseFileIO(upath_obj=mock_upath)
            assert fileio.file_extension == expected_ext


class TestBaseFileIOFileInfo:
    """Test BaseFileIO._finfo method."""
    
    def test_finfo_returns_file_information(self, mock_upath):
        """Test that _finfo returns file system information."""
        expected_info = {"size": 1024, "type": "file", "mtime": 1609459200}
        mock_upath.suffix = ".txt"
        mock_upath.fs.info.return_value = expected_info
        
        fileio = BaseFileIO(upath_obj=mock_upath)
        result = fileio._finfo()
        
        mock_upath.fs.info.assert_called_once_with(mock_upath.path)
        assert result == expected_info

    def test_finfo_passes_additional_arguments(self, mock_upath):
        """Test that _finfo passes additional arguments to fs.info."""
        mock_upath.suffix = ".txt"
        mock_upath.fs.info.return_value = {}
        
        fileio = BaseFileIO(upath_obj=mock_upath)
        fileio._finfo("extra_arg", detail="full")
        
        mock_upath.fs.info.assert_called_once_with(
            mock_upath.path, "extra_arg", detail="full"
        )

    def test_finfo_handles_os_error_with_warning(self, mock_upath):
        """Test that _finfo handles OSError and raises with warning."""
        mock_upath.suffix = ".txt"
        mock_upath.path = "/nonexistent/file.txt"
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
        mock_upath.suffix = ".json"
        mock_upath.exists.return_value = False
        mock_upath.path = "/test/file.json"
        
        fileio = BaseFileIO(upath_obj=mock_upath)
        
        with pytest.raises(FileNotFoundError, match="File not found: /test/file.json"):
            fileio._fread()

    def test_fread_opens_file_in_binary_mode(self, mock_upath):
        """Test that _fread opens file in binary mode."""
        mock_upath.suffix = ".json"
        mock_upath.exists.return_value = True
        mock_file_content = b'{"key": "value"}'
        
        # Mock the file system open call
        mock_file = MagicMock()
        mock_file.read.return_value = mock_file_content
        mock_upath.fs.open.return_value.__enter__.return_value = mock_file
        
        # Mock the JSON file IO class
        with patch('src.main.file_io._base.fileio_mapping') as mock_mapping:
            mock_json_io = MagicMock()
            mock_json_io._read.return_value = {"key": "value"}
            mock_mapping.__getitem__.return_value = mock_json_io
            
            fileio = BaseFileIO(upath_obj=mock_upath)
            result = fileio._fread()
            
            # Verify file opened in binary mode
            mock_upath.fs.open.assert_called_once_with(mock_upath.path, 'rb')
            
            # Verify file IO class was used
            mock_json_io._read.assert_called_once()
            assert result == {"key": "value"}

    def test_fread_uses_correct_fileio_class(self, mock_upath):
        """Test that _fread uses the correct file IO class based on extension."""
        extensions_to_test = ["json", "csv", "txt", "yaml"]
        
        for ext in extensions_to_test:
            mock_upath.suffix = f".{ext}"
            mock_upath.exists.return_value = True
            
            mock_file = MagicMock()
            mock_file.read.return_value = b"test content"
            mock_upath.fs.open.return_value.__enter__.return_value = mock_file
            
            with patch('src.main.file_io._base.fileio_mapping') as mock_mapping:
                mock_io_class = MagicMock()
                mock_io_class._read.return_value = f"parsed_{ext}_data"
                mock_mapping.__getitem__.return_value = mock_io_class
                
                fileio = BaseFileIO(upath_obj=mock_upath)
                result = fileio._fread()
                
                # Verify correct file IO class was accessed
                mock_mapping.__getitem__.assert_called_with(ext)
                mock_io_class._read.assert_called_once()
                assert result == f"parsed_{ext}_data"


class TestBaseFileIOFileCopy:
    """Test BaseFileIO._copy method."""
    
    def test_copy_checks_source_file_exists(self, mock_upath):
        """Test that _copy checks if source file exists."""
        mock_upath.suffix = ".txt"
        mock_upath.exists.return_value = False
        mock_upath.path = "/source/file.txt"
        
        fileio = BaseFileIO(upath_obj=mock_upath)
        
        with pytest.raises(FileNotFoundError, match="Source file not found"):
            fileio._copy(dest_path="/dest/file.txt")

    def test_copy_validates_destination_path(self, mock_upath):
        """Test that _copy validates destination path is not empty."""
        mock_upath.suffix = ".txt"
        mock_upath.exists.return_value = True
        
        fileio = BaseFileIO(upath_obj=mock_upath)
        
        # Test empty string
        with pytest.raises(ValueError, match="Destination path cannot be empty"):
            fileio._copy(dest_path="")
        
        # Test whitespace only
        with pytest.raises(ValueError, match="Destination path cannot be empty"):
            fileio._copy(dest_path="   ")

    def test_copy_performs_file_copy_operation(self, mock_upath):
        """Test that _copy performs the actual file copying."""
        mock_upath.suffix = ".txt"
        mock_upath.exists.return_value = True
        mock_upath.path = "/source/file.txt"
        
        # Mock source file content
        source_content = b"test file content"
        mock_src_file = MagicMock()
        mock_src_file.read.return_value = source_content
        
        # Mock destination file
        mock_dest_file = MagicMock()
        
        # Mock file system operations
        mock_upath.fs.open.side_effect = [
            mock_src_file,  # Source file open
            mock_dest_file  # Destination file open
        ]
        
        # Mock destination UPath creation
        with patch('src.main.file_io._base.UPath') as mock_upath_class:
            mock_dest_upath = MagicMock()
            mock_dest_upath.path = "/dest/file.txt"
            mock_dest_upath.fs = mock_upath.fs  # Same filesystem
            mock_upath_class.return_value = mock_dest_upath
            
            fileio = BaseFileIO(upath_obj=mock_upath)
            fileio._copy(dest_path="/dest/file.txt")
            
            # Verify source file was opened for reading
            assert call(mock_upath.path, 'rb') in mock_upath.fs.open.call_args_list
            
            # Verify destination file was opened for writing
            assert call("/dest/file.txt", 'wb') in mock_upath.fs.open.call_args_list
            
            # Verify content was written to destination
            mock_dest_file.write.assert_called_once_with(source_content)

    def test_copy_handles_os_error_with_warning(self, mock_upath):
        """Test that _copy handles OSError and raises with warning."""
        mock_upath.suffix = ".txt"
        mock_upath.exists.return_value = True
        mock_upath.path = "/source/file.txt"
        
        # Mock OS error during file operations
        mock_upath.fs.open.side_effect = OSError("Permission denied")
        
        fileio = BaseFileIO(upath_obj=mock_upath)
        
        with pytest.raises(OSError):
            with warnings.catch_warnings(record=True) as w:
                fileio._copy(dest_path="/dest/file.txt")
                
                # Verify warning was issued
                assert len(w) == 1
                assert "Failed to copy" in str(w[0].message)


class TestBaseFileIOFileWrite:
    """Test BaseFileIO._fwrite method."""
    
    def test_fwrite_validates_data_type_before_writing(self, mock_upath):
        """Test that _fwrite validates data type before attempting write."""
        mock_upath.suffix = ".csv"
        
        fileio = BaseFileIO(upath_obj=mock_upath)
        
        with patch.object(fileio, '_validate_data_type') as mock_validate:
            with patch('src.main.file_io._base.fileio_mapping') as mock_mapping:
                mock_csv_io = MagicMock()
                mock_mapping.__getitem__.return_value = mock_csv_io
                
                test_data = "invalid data for csv"
                fileio._fwrite(data=test_data)
                
                # Verify validation was called with correct parameters
                mock_validate.assert_called_once_with(test_data, "csv")

    def test_fwrite_uses_correct_fileio_class(self, mock_upath):
        """Test that _fwrite uses correct file IO class based on extension."""
        mock_upath.suffix = ".json"
        
        test_data = {"key": "value"}
        
        with patch('src.main.file_io._base.fileio_mapping') as mock_mapping:
            mock_json_io = MagicMock()
            mock_mapping.__getitem__.return_value = mock_json_io
            
            fileio = BaseFileIO(upath_obj=mock_upath)
            fileio._fwrite(data=test_data)
            
            # Verify correct file IO class was accessed
            mock_mapping.__getitem__.assert_called_with("json")
            
            # Verify write method was called with correct parameters
            mock_json_io._write.assert_called_once_with(mock_upath, test_data)

    def test_fwrite_passes_additional_arguments(self, mock_upath):
        """Test that _fwrite passes additional arguments to file IO class."""
        mock_upath.suffix = ".txt"
        
        with patch('src.main.file_io._base.fileio_mapping') as mock_mapping:
            mock_txt_io = MagicMock()
            mock_mapping.__getitem__.return_value = mock_txt_io
            
            fileio = BaseFileIO(upath_obj=mock_upath)
            fileio._fwrite(data="test", encoding="utf-8", mode="w")
            
            # Verify additional arguments passed to write method
            mock_txt_io._write.assert_called_once_with(
                mock_upath, "test", encoding="utf-8", mode="w"
            )


class TestBaseFileIODataValidation:
    """Test BaseFileIO._validate_data_type method."""
    
    def test_validate_dataframe_formats_require_dataframe(self, mock_upath):
        """Test that DataFrame formats require pandas DataFrame."""
        import pandas as pd
        
        mock_upath.suffix = ".csv"
        fileio = BaseFileIO(upath_obj=mock_upath)
        
        # Valid DataFrame should pass
        valid_df = pd.DataFrame({"col": [1, 2, 3]})
        fileio._validate_data_type(valid_df, "csv")  # Should not raise
        
        # Non-DataFrame should raise TypeError
        with pytest.raises(TypeError, match="requires a pandas DataFrame"):
            fileio._validate_data_type("not a dataframe", "csv")

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

    @pytest.mark.parametrize("file_extension", ["txt", "text", "sql"])
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
    
    def test_fmakedirs_calls_filesystem_makedirs(self, mock_upath):
        """Test that _fmakedirs calls filesystem makedirs method."""
        mock_upath.suffix = ".txt"
        
        fileio = BaseFileIO(upath_obj=mock_upath)
        test_dir = "/test/new/directory"
        
        fileio._fmakedirs(dirpath=test_dir)
        
        # Verify filesystem makedirs was called with correct parameters
        mock_upath.fs.makedirs.assert_called_once_with(test_dir, exist_ok=True)

    def test_fmakedirs_respects_exist_ok_parameter(self, mock_upath):
        """Test that _fmakedirs respects the exist_ok parameter."""
        mock_upath.suffix = ".txt"
        
        fileio = BaseFileIO(upath_obj=mock_upath)
        test_dir = "/test/directory"
        
        fileio._fmakedirs(dirpath=test_dir, exist_ok=False)
        
        # Verify exist_ok parameter was passed correctly
        mock_upath.fs.makedirs.assert_called_once_with(test_dir, exist_ok=False)

    def test_fmakedirs_handles_os_error_with_warning(self, mock_upath):
        """Test that _fmakedirs handles OSError and raises with warning."""
        mock_upath.suffix = ".txt"
        mock_upath.fs.makedirs.side_effect = OSError("Permission denied")
        
        fileio = BaseFileIO(upath_obj=mock_upath)
        
        with pytest.raises(OSError):
            with warnings.catch_warnings(record=True) as w:
                fileio._fmakedirs(dirpath="/test/dir")
                
                # Verify warning was issued
                assert len(w) == 1
                assert "Failed to create directories" in str(w[0].message)

    def test_fdelete_validates_filepath_not_empty(self, mock_upath):
        """Test that _fdelete validates filepath is not empty."""
        mock_upath.suffix = ".txt"
        
        fileio = BaseFileIO(upath_obj=mock_upath)
        
        # Test empty string
        with pytest.raises(ValueError, match="File path cannot be empty"):
            fileio._fdelete(filepath="")
        
        # Test whitespace only
        with pytest.raises(ValueError, match="File path cannot be empty"):
            fileio._fdelete(filepath="   ")

    def test_fdelete_warns_for_nonexistent_files(self, mock_upath):
        """Test that _fdelete warns but doesn't error for non-existent files."""
        mock_upath.suffix = ".txt"
        
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
                
                # Verify filesystem delete was NOT called
                mock_upath.fs.delete.assert_not_called()

    def test_fdelete_calls_filesystem_delete_for_existing_files(self, mock_upath):
        """Test that _fdelete calls filesystem delete for existing files."""
        mock_upath.suffix = ".txt"
        
        fileio = BaseFileIO(upath_obj=mock_upath)
        test_path = "/test/file.txt"
        
        with patch('src.main.file_io._base.UPath') as mock_upath_class:
            mock_delete_path = MagicMock()
            mock_delete_path.exists.return_value = True
            mock_upath_class.return_value = mock_delete_path
            
            fileio._fdelete(filepath=test_path)
            
            # Verify filesystem delete was called
            mock_upath.fs.delete.assert_called_once_with(test_path)

    def test_fdelete_handles_os_error_with_warning(self, mock_upath):
        """Test that _fdelete handles OSError and raises with warning."""
        mock_upath.suffix = ".txt"
        mock_upath.fs.delete.side_effect = OSError("Permission denied")
        
        fileio = BaseFileIO(upath_obj=mock_upath)
        
        with patch('src.main.file_io._base.UPath') as mock_upath_class:
            mock_delete_path = MagicMock()
            mock_delete_path.exists.return_value = True
            mock_upath_class.return_value = mock_delete_path
            
            with pytest.raises(OSError):
                with warnings.catch_warnings(record=True) as w:
                    fileio._fdelete(filepath="/test/file.txt")
                    
                    # Verify warning was issued
                    assert len(w) == 1
                    assert "Failed to delete" in str(w[0].message)
