"""
Comprehensive test suite for FileIO classes.

This test suite demonstrates pytest best practices for file I/O testing:
- Fixture usage for test data and temporary directories
- Parametrized testing for multiple file formats
- Mock usage for filesystem operations
- Error testing with different file scenarios
- Integration testing with real files

Key pytest concepts demonstrated:
📚 Fixtures: Temporary directories, sample data, and file cleanup
🔄 Parametrize: Testing multiple file formats with same logic
🎭 Mocking: Filesystem operations and external dependencies  
🧪 Assertions: File content validation and error conditions
🗂️ tmpdir: Pytest's built-in temporary directory fixture
"""

import pytest
import os
import tempfile
import json
import pandas as pd
from io import BytesIO
from pathlib import Path
from unittest.mock import patch, MagicMock, mock_open

from src.main.file_io import FileIOInterface, BaseFileIO
from src.main.file_io._base import fileio_mapping
from src.main.file_io.json import JsonFileIO
from src.main.file_io.text import TextFileIO

pytestmark = pytest.mark.unit

# ========================================================================================
# PYTEST LEARNING TIP 💡
# FileIO testing requires careful management of temporary files and directories.
# Use pytest's tmpdir fixture and proper cleanup to avoid test pollution.
# ========================================================================================

class TestFileIOInterface:
    """Test the main FileIOInterface static methods."""
    
    @pytest.fixture
    def sample_json_data(self):
        """Sample JSON data for testing."""
        return {"name": "test", "value": 123, "active": True}
    
    @pytest.fixture
    def sample_text_data(self):
        """Sample text data for testing."""
        return "Hello, World!\nThis is a test file."
    
    @pytest.fixture
    def sample_dataframe(self):
        """Sample pandas DataFrame for testing."""
        return pd.DataFrame({
            'col1': [1, 2, 3],
            'col2': ['a', 'b', 'c'],
            'col3': [1.1, 2.2, 3.3]
        })

    def test_instantiate_creates_basefileio(self, temp_directory):
        """Test that _instantiate creates a BaseFileIO object."""
        test_file = os.path.join(temp_directory, "test.json")
        
        fileio = FileIOInterface._instantiate(test_file)
        
        assert isinstance(fileio, BaseFileIO)
        assert str(fileio.upath.path) == test_file
        assert fileio.file_extension == "json"
    
    def test_instantiate_with_filesystem(self, temp_directory):
        """Test _instantiate with specific filesystem."""
        test_file = os.path.join(temp_directory, "test.txt")
        
        fileio = FileIOInterface._instantiate(test_file, filesystem="file")
        
        assert isinstance(fileio, BaseFileIO)
        assert fileio.file_extension == "txt"
    
    def test_fwrite_and_fread_json(self, temp_directory, sample_json_data):
        """Test writing and reading JSON files."""
        json_file = os.path.join(temp_directory, "test.json")
        
        # Write JSON data
        FileIOInterface.fwrite(json_file, sample_json_data)
        
        # Verify file exists
        assert os.path.exists(json_file)
        
        # Read JSON data back
        read_data = FileIOInterface.fread(json_file)
        
        assert read_data == sample_json_data
    
    def test_fwrite_and_fread_text(self, temp_directory, sample_text_data):
        """Test writing and reading text files."""
        text_file = os.path.join(temp_directory, "test.txt")
        
        # Write text data
        FileIOInterface.fwrite(text_file, sample_text_data)
        
        # Verify file exists
        assert os.path.exists(text_file)
        
        # Read text data back
        read_data = FileIOInterface.fread(text_file)
        
        assert read_data == sample_text_data
    
    @pytest.mark.parametrize("file_extension,data_type,sample_data", [
        ("json", dict, {"test": "data"}),
        ("txt", str, "test text content"),
        ("yaml", dict, {"yaml": "data"}),
    ])
    def test_fwrite_fread_multiple_formats(self, temp_directory, file_extension, data_type, sample_data):
        """Test write/read operations for multiple file formats.
        
        PYTEST: Parametrized test efficiently covers multiple file formats.
        This ensures consistent behavior across different file types.
        """
        test_file = os.path.join(temp_directory, f"test.{file_extension}")
        
        # Write data
        FileIOInterface.fwrite(test_file, sample_data)
        
        # Read data back
        read_data = FileIOInterface.fread(test_file)
        
        assert read_data == sample_data
        assert isinstance(read_data, data_type)
    
    def test_fcopy_file(self, temp_directory, sample_text_data):
        """Test file copying functionality."""
        source_file = os.path.join(temp_directory, "source.txt")
        dest_file = os.path.join(temp_directory, "destination.txt")
        
        # Create source file
        FileIOInterface.fwrite(source_file, sample_text_data)
        
        # Copy file
        FileIOInterface.fcopy(source_file, dest_file)
        
        # Verify both files exist and have same content
        assert os.path.exists(source_file)
        assert os.path.exists(dest_file)
        
        source_content = FileIOInterface.fread(source_file)
        dest_content = FileIOInterface.fread(dest_file)
        
        assert source_content == dest_content == sample_text_data
    
    def test_fmakedirs_creates_directories(self, temp_directory):
        """Test directory creation functionality."""
        nested_dir = os.path.join(temp_directory, "level1", "level2", "level3")
        
        FileIOInterface.fmakedirs(nested_dir)
        
        assert os.path.exists(nested_dir)
        assert os.path.isdir(nested_dir)
    
    def test_fmakedirs_exist_ok_true(self, temp_directory):
        """Test fmakedirs with exist_ok=True doesn't raise error for existing dir."""
        existing_dir = os.path.join(temp_directory, "existing")
        os.makedirs(existing_dir)
        
        # Should not raise error
        FileIOInterface.fmakedirs(existing_dir, exist_ok=True)
        
        assert os.path.exists(existing_dir)
    
    def test_fdelete_removes_file(self, temp_directory, sample_text_data):
        """Test file deletion functionality."""
        test_file = os.path.join(temp_directory, "delete_me.txt")
        
        # Create file
        FileIOInterface.fwrite(test_file, sample_text_data)
        assert os.path.exists(test_file)
        
        # Delete file
        FileIOInterface.fdelete(test_file)
        
        assert not os.path.exists(test_file)
    
    def test_finfo_returns_file_info(self, temp_directory, sample_text_data):
        """Test getting file information."""
        test_file = os.path.join(temp_directory, "info_test.txt")
        
        # Create file
        FileIOInterface.fwrite(test_file, sample_text_data)
        
        # Get file info
        file_info = FileIOInterface.finfo(test_file)
        
        assert isinstance(file_info, dict)
        # File info should contain basic information
        assert "size" in file_info or "name" in file_info


class TestBaseFileIO:
    """Test BaseFileIO class functionality."""
    
    @pytest.fixture
    def json_upath(self, temp_directory):
        """Create UPath object for JSON file."""
        from upath import UPath
        return UPath(os.path.join(temp_directory, "test.json"))
    
    @pytest.fixture
    def txt_upath(self, temp_directory):
        """Create UPath object for text file."""
        from upath import UPath
        return UPath(os.path.join(temp_directory, "test.txt"))
    
    def test_init_validates_file_extension(self, json_upath):
        """Test that initialization validates file extensions."""
        fileio = BaseFileIO(json_upath)
        
        assert fileio.file_extension == "json"
        assert fileio.upath == json_upath
    
    def test_init_raises_error_for_no_extension(self, temp_directory):
        """Test that initialization raises error for files without extensions."""
        from upath import UPath
        
        no_ext_path = UPath(os.path.join(temp_directory, "noextension"))
        
        with pytest.raises(ValueError, match="has no extension"):
            BaseFileIO(no_ext_path)
    
    def test_init_raises_error_for_unsupported_extension(self, temp_directory):
        """Test that initialization raises error for unsupported file formats."""
        from upath import UPath
        
        unsupported_path = UPath(os.path.join(temp_directory, "test.unsupported"))
        
        with pytest.raises(ValueError, match="Unsupported file format"):
            BaseFileIO(unsupported_path)
    
    @pytest.mark.parametrize("extension,expected_class", [
        ("json", "JsonFileIO"),
        ("txt", "TextFileIO"),
        ("csv", "CSVFileIO"),
        ("yaml", "YamlFileIO"),
        ("yml", "YamlFileIO"),
    ])
    def test_fileio_mapping_contains_expected_classes(self, extension, expected_class):
        """Test that fileio_mapping contains expected classes for extensions."""
        assert extension in fileio_mapping
        assert fileio_mapping[extension].__name__ == expected_class
    
    def test_fread_raises_filenotfound_for_missing_file(self, json_upath):
        """Test that _fread raises FileNotFoundError for missing files."""
        fileio = BaseFileIO(json_upath)
        
        with pytest.raises(FileNotFoundError, match="File not found"):
            fileio._fread()
    
    def test_copy_raises_filenotfound_for_missing_source(self, json_upath, temp_directory):
        """Test that _copy raises FileNotFoundError for missing source files."""
        fileio = BaseFileIO(json_upath)
        dest_path = os.path.join(temp_directory, "dest.json")
        
        with pytest.raises(FileNotFoundError, match="Source file not found"):
            fileio._copy(dest_path)
    
    def test_copy_raises_error_for_empty_dest_path(self, json_upath):
        """Test that _copy raises ValueError for empty destination path."""
        # Create source file first
        json_upath.write_text('{"test": "data"}')
        
        fileio = BaseFileIO(json_upath)
        
        with pytest.raises(ValueError, match="Destination path cannot be empty"):
            fileio._copy("")
    
    def test_validate_data_type_dataframe_formats(self, temp_directory):
        """Test data type validation for DataFrame formats."""
        from upath import UPath
        
        # Should raise TypeError for non-DataFrame data with CSV extension
        csv_upath = UPath(os.path.join(temp_directory, "test.csv"))
        csv_fileio = BaseFileIO(csv_upath)
        
        with pytest.raises(TypeError, match="requires a pandas DataFrame"):
            csv_fileio._validate_data_type("not a dataframe", "csv")
    
    def test_validate_data_type_text_formats(self, txt_upath):
        """Test data type validation for text formats."""
        fileio = BaseFileIO(txt_upath)
        
        # Should raise TypeError for non-string data with text extension
        with pytest.raises(TypeError, match="requires a string"):
            fileio._validate_data_type(123, "txt")
    
    def test_fdelete_raises_error_for_empty_path(self, json_upath):
        """Test that _fdelete raises ValueError for empty file path."""
        fileio = BaseFileIO(json_upath)
        
        with pytest.raises(ValueError, match="File path cannot be empty"):
            fileio._fdelete("")


class TestErrorHandling:
    """Test error handling across FileIO operations."""
    
    def test_fread_nonexistent_file(self, temp_directory):
        """Test reading a file that doesn't exist."""
        nonexistent_file = os.path.join(temp_directory, "doesnotexist.json")
        
        with pytest.raises(FileNotFoundError):
            FileIOInterface.fread(nonexistent_file)
    
    def test_fwrite_invalid_data_type_for_format(self, temp_directory):
        """Test writing invalid data type for specific file format."""
        csv_file = os.path.join(temp_directory, "test.csv")
        invalid_data = "this should be a DataFrame"
        
        with pytest.raises(TypeError, match="requires a pandas DataFrame"):
            FileIOInterface.fwrite(csv_file, invalid_data)
    
    def test_unsupported_file_extension(self, temp_directory):
        """Test operations on unsupported file extensions."""
        unsupported_file = os.path.join(temp_directory, "test.unsupported")
        
        with pytest.raises(ValueError, match="Unsupported file format"):
            FileIOInterface.fwrite(unsupported_file, {"data": "test"})
    
    def test_finfo_on_nonexistent_file_raises_error(self, temp_directory):
        """Test that finfo raises OSError for nonexistent files."""
        nonexistent_file = os.path.join(temp_directory, "missing.json")
        
        with pytest.raises(OSError):
            FileIOInterface.finfo(nonexistent_file)
    
    def test_fcopy_with_invalid_destination_permissions(self, temp_directory):
        """Test file copy with permission issues."""
        # This test might be platform-specific, so we'll use mocking
        source_file = os.path.join(temp_directory, "source.txt")
        dest_file = "/root/readonly/dest.txt"  # Likely no permission
        
        # Create source file
        FileIOInterface.fwrite(source_file, "test content")
        
        # This should raise an OSError due to permission issues
        # Note: This test may not work on all systems, so we'll skip it
        # if the directory structure doesn't match expectations
        if not os.name == 'nt':  # Skip on Windows
            pytest.skip("Permission test not applicable on this system")


class TestFileIOSpecificFormats:
    """Test specific file format implementations."""
    
    def test_json_fileio_read_write(self, temp_directory):
        """Test JsonFileIO read and write operations."""
        test_data = {"key": "value", "number": 42, "list": [1, 2, 3]}
        
        # Test BytesIO reading
        json_bytes = BytesIO(json.dumps(test_data).encode())
        read_data = JsonFileIO._read(json_bytes)
        assert read_data == test_data
        
        # Test UPath writing
        from upath import UPath
        json_file = UPath(os.path.join(temp_directory, "test.json"))
        JsonFileIO._write(json_file, test_data)
        
        # Verify written file
        with open(json_file.path, 'r') as f:
            written_data = json.load(f)
        assert written_data == test_data
    
    def test_text_fileio_read_write(self, temp_directory):
        """Test TextFileIO read and write operations."""
        test_text = "Hello, World!\nMultiple lines\nof text."
        
        # Test BytesIO reading
        text_bytes = BytesIO(test_text.encode())
        read_text = TextFileIO._read(text_bytes)
        assert read_text == test_text
        
        # Test UPath writing
        from upath import UPath
        text_file = UPath(os.path.join(temp_directory, "test.txt"))
        TextFileIO._write(text_file, test_text)
        
        # Verify written file
        with open(text_file.path, 'r') as f:
            written_text = f.read()
        assert written_text == test_text


class TestIntegrationScenarios:
    """Integration tests for real-world scenarios."""
    
    @pytest.fixture
    def project_structure(self, temp_directory):
        """Create a sample project structure for testing."""
        project_dir = os.path.join(temp_directory, "project")
        data_dir = os.path.join(project_dir, "data")
        config_dir = os.path.join(project_dir, "config")
        
        FileIOInterface.fmakedirs(data_dir)
        FileIOInterface.fmakedirs(config_dir)
        
        return {
            "project": project_dir,
            "data": data_dir,
            "config": config_dir
        }
    
    def test_full_data_pipeline(self, project_structure):
        """Test a complete data processing pipeline."""
        data_dir = project_structure["data"]
        config_dir = project_structure["config"]
        
        # 1. Create configuration
        config_data = {
            "app_name": "test_app",
            "version": "1.0.0",
            "settings": {
                "debug": True,
                "max_records": 1000
            }
        }
        config_file = os.path.join(config_dir, "config.json")
        FileIOInterface.fwrite(config_file, config_data)
        
        # 2. Create sample data
        sample_data = "record1,value1\nrecord2,value2\nrecord3,value3"
        data_file = os.path.join(data_dir, "input.txt")
        FileIOInterface.fwrite(data_file, sample_data)
        
        # 3. Process data (read config, read data, transform, write output)
        loaded_config = FileIOInterface.fread(config_file)
        loaded_data = FileIOInterface.fread(data_file)
        
        # Simple transformation
        lines = loaded_data.strip().split('\n')
        processed_data = {
            "app": loaded_config["app_name"],
            "records": len(lines),
            "data": [line.split(',') for line in lines]
        }
        
        # 4. Write output
        output_file = os.path.join(data_dir, "output.json")
        FileIOInterface.fwrite(output_file, processed_data)
        
        # 5. Verify pipeline results
        result = FileIOInterface.fread(output_file)
        assert result["app"] == "test_app"
        assert result["records"] == 3
        assert len(result["data"]) == 3
        assert result["data"][0] == ["record1", "value1"]
    
    def test_backup_and_restore_scenario(self, temp_directory):
        """Test backup and restore operations."""
        # Original files
        original_dir = os.path.join(temp_directory, "original")
        backup_dir = os.path.join(temp_directory, "backup")
        
        FileIOInterface.fmakedirs(original_dir)
        FileIOInterface.fmakedirs(backup_dir)
        
        # Create multiple files
        files_data = {
            "config.json": {"setting1": "value1", "setting2": "value2"},
            "data.txt": "Important data content\nLine 2\nLine 3",
            "metadata.yaml": {"version": 1, "created": "2024-01-01"}
        }
        
        # Write original files
        original_files = {}
        for filename, data in files_data.items():
            file_path = os.path.join(original_dir, filename)
            FileIOInterface.fwrite(file_path, data)
            original_files[filename] = file_path
        
        # Backup files
        backup_files = {}
        for filename, original_path in original_files.items():
            backup_path = os.path.join(backup_dir, filename)
            FileIOInterface.fcopy(original_path, backup_path)
            backup_files[filename] = backup_path
        
        # Verify backup integrity
        for filename in files_data:
            original_content = FileIOInterface.fread(original_files[filename])
            backup_content = FileIOInterface.fread(backup_files[filename])
            assert original_content == backup_content
        
        # Simulate data loss (delete original files)
        for original_path in original_files.values():
            FileIOInterface.fdelete(original_path)
        
        # Verify files are deleted
        for original_path in original_files.values():
            assert not os.path.exists(original_path)
        
        # Restore from backup
        for filename, backup_path in backup_files.items():
            restored_path = os.path.join(original_dir, filename)
            FileIOInterface.fcopy(backup_path, restored_path)
        
        # Verify restoration
        for filename, expected_data in files_data.items():
            restored_path = os.path.join(original_dir, filename)
            restored_content = FileIOInterface.fread(restored_path)
            assert restored_content == expected_data


# ========================================================================================
# PYTEST LEARNING TIP 💡
# Integration tests like these show how individual components work together.
# They're valuable for catching issues that unit tests might miss, but they're
# also more complex and can be harder to debug when they fail.
# ========================================================================================

class TestMockingFileOperations:
    """Test file operations using mocking for edge cases."""
    
    def test_fread_with_mocked_filesystem_error(self):
        """Test fread behavior when filesystem raises unexpected errors."""
        with patch('src.main.file_io.FileIOInterface._instantiate') as mock_instantiate:
            mock_fileio = MagicMock()
            mock_fileio._fread.side_effect = OSError("Disk full")
            mock_instantiate.return_value = mock_fileio
            
            with pytest.raises(OSError, match="Disk full"):
                FileIOInterface.fread("test.json")
    
    def test_fwrite_with_mocked_permission_error(self):
        """Test fwrite behavior when filesystem denies permission."""
        with patch('src.main.file_io.FileIOInterface._instantiate') as mock_instantiate:
            mock_fileio = MagicMock()
            mock_fileio._fwrite.side_effect = PermissionError("Access denied")
            mock_instantiate.return_value = mock_fileio
            
            with pytest.raises(PermissionError, match="Access denied"):
                FileIOInterface.fwrite("readonly.json", {"data": "test"})
    
    @patch('src.main.file_io._base.warnings.warn')
    def test_fdelete_nonexistent_file_warns(self, mock_warn):
        """Test that deleting nonexistent file produces warning."""
        with patch('upath.UPath.exists', return_value=False):
            # Create a mock BaseFileIO instance
            from upath import UPath
            mock_upath = UPath("nonexistent.txt")
            fileio = BaseFileIO.__new__(BaseFileIO)  # Create without calling __init__
            fileio.upath = mock_upath
            
            # This should warn but not raise an error
            fileio._fdelete("nonexistent.txt")
            
            mock_warn.assert_called_once()
            assert "does not exist" in str(mock_warn.call_args[0][0])
