"""
Integration test suite for FileIO functionality.

These tests use real files and file system operations to validate
the complete FileIO workflow. Tests are designed to be Fast, Independent,
Repeatable, Self-Validating, and Timely (FIRST principles).

Integration test areas:
📁 Real file creation and reading
📝 Data roundtrip testing (write then read)
🔄 File format conversions
📋 File copying operations
📂 Directory management
🗑️ File deletion operations
"""

import pytest
import os
import json
import yaml
import tempfile
import pandas as pd
from pathlib import Path

from src.main.file_io import FileIOInterface

pytestmark = pytest.mark.integration


class TestFileIOIntegrationRoundtrip:
    """Integration tests for complete write-read cycles."""
    
    def test_json_roundtrip_integration(self, temp_dir, sample_json_data):
        """Test complete JSON write-read cycle with real files.
        
        INTEGRATION: This test uses real files and verifies that data
        written to a JSON file can be read back correctly.
        """
        json_path = os.path.join(temp_dir, "test_roundtrip.json")
        
        # Write data to JSON file
        FileIOInterface.fwrite(write_path=json_path, data=sample_json_data)
        
        # Verify file was created
        assert os.path.exists(json_path)
        
        # Read data back from JSON file
        read_data = FileIOInterface.fread(read_path=json_path)
        
        # Verify data roundtrip is correct
        assert read_data == sample_json_data

    def test_yaml_roundtrip_integration(self, temp_dir, sample_yaml_data):
        """Test complete YAML write-read cycle with real files."""
        yaml_path = os.path.join(temp_dir, "test_roundtrip.yaml")
        
        # Write data to YAML file
        FileIOInterface.fwrite(write_path=yaml_path, data=sample_yaml_data)
        
        # Verify file was created
        assert os.path.exists(yaml_path)
        
        # Read data back from YAML file
        read_data = FileIOInterface.fread(read_path=yaml_path)
        
        # Verify data roundtrip is correct
        assert read_data == sample_yaml_data

    def test_text_roundtrip_integration(self, temp_dir, sample_text_data):
        """Test complete text write-read cycle with real files."""
        txt_path = os.path.join(temp_dir, "test_roundtrip.txt")
        
        # Write data to text file
        FileIOInterface.fwrite(write_path=txt_path, data=sample_text_data)
        
        # Verify file was created
        assert os.path.exists(txt_path)
        
        # Read data back from text file
        read_data = FileIOInterface.fread(read_path=txt_path)
        
        # Verify data roundtrip is correct
        assert read_data == sample_text_data

    def test_csv_roundtrip_integration(self, temp_dir, sample_dataframe):
        """Test complete CSV write-read cycle with real files."""
        csv_path = os.path.join(temp_dir, "test_roundtrip.csv")
        
        # Write DataFrame to CSV file
        FileIOInterface.fwrite(write_path=csv_path, data=sample_dataframe)
        
        # Verify file was created
        assert os.path.exists(csv_path)
        
        # Read DataFrame back from CSV file
        read_df = FileIOInterface.fread(read_path=csv_path)
        
        # Verify DataFrame roundtrip is correct
        pd.testing.assert_frame_equal(read_df, sample_dataframe)

    def test_pickle_roundtrip_integration(self, temp_dir):
        """Test complete Pickle write-read cycle with complex data."""
        pickle_path = os.path.join(temp_dir, "test_roundtrip.pkl")
        
        # Create complex data structure
        complex_data = {
            'dataframe': pd.DataFrame({'a': [1, 2, 3], 'b': [4, 5, 6]}),
            'list': [1, 2, 3, 'string', True],
            'nested': {'inner': {'value': 42}},
            'tuple': (1, 2, 3)
        }
        
        # Write data to pickle file
        FileIOInterface.fwrite(write_path=pickle_path, data=complex_data)
        
        # Verify file was created
        assert os.path.exists(pickle_path)
        
        # Read data back from pickle file
        read_data = FileIOInterface.fread(read_path=pickle_path)
        
        # Verify complex data roundtrip
        assert read_data['list'] == complex_data['list']
        assert read_data['nested'] == complex_data['nested']
        assert read_data['tuple'] == complex_data['tuple']
        pd.testing.assert_frame_equal(read_data['dataframe'], complex_data['dataframe'])


class TestFileIOIntegrationFileCopy:
    """Integration tests for file copying operations."""
    
    def test_copy_json_file_integration(self, temp_dir, sample_json_data):
        """Test copying JSON files with real file operations."""
        source_path = os.path.join(temp_dir, "source.json")
        dest_path = os.path.join(temp_dir, "destination.json")
        
        # Create source file
        FileIOInterface.fwrite(write_path=source_path, data=sample_json_data)
        assert os.path.exists(source_path)
        
        # Copy file
        FileIOInterface.fcopy(read_path=source_path, dest_path=dest_path)
        
        # Verify destination file exists
        assert os.path.exists(dest_path)
        
        # Verify copied content is identical
        source_data = FileIOInterface.fread(read_path=source_path)
        dest_data = FileIOInterface.fread(read_path=dest_path)
        assert source_data == dest_data == sample_json_data

    def test_copy_preserves_file_content_across_formats(self, temp_dir):
        """Test that file copying preserves content for different formats."""
        test_cases = [
            ("test.txt", "Text file content for copying test"),
            ("config.yaml", {"app": {"name": "test", "version": "1.0"}}),
            ("data.json", {"users": [{"id": 1, "name": "Alice"}]})
        ]
        
        for filename, data in test_cases:
            source_path = os.path.join(temp_dir, f"source_{filename}")
            dest_path = os.path.join(temp_dir, f"dest_{filename}")
            
            # Create source file
            FileIOInterface.fwrite(write_path=source_path, data=data)
            
            # Copy file
            FileIOInterface.fcopy(read_path=source_path, dest_path=dest_path)
            
            # Verify content preservation
            copied_data = FileIOInterface.fread(read_path=dest_path)
            assert copied_data == data

    def test_copy_to_different_directory_integration(self, temp_dir):
        """Test copying files to different directories."""
        # Create subdirectories
        source_dir = os.path.join(temp_dir, "source_dir")
        dest_dir = os.path.join(temp_dir, "dest_dir")
        
        # Use fmakedirs to create directories
        FileIOInterface.fmakedirs(path=source_dir)
        FileIOInterface.fmakedirs(path=dest_dir)
        
        source_path = os.path.join(source_dir, "file.json")
        dest_path = os.path.join(dest_dir, "copied_file.json")
        
        test_data = {"test": "cross directory copy"}
        
        # Create source file
        FileIOInterface.fwrite(write_path=source_path, data=test_data)
        
        # Copy across directories
        FileIOInterface.fcopy(read_path=source_path, dest_path=dest_path)
        
        # Verify copy successful
        assert os.path.exists(dest_path)
        copied_data = FileIOInterface.fread(read_path=dest_path)
        assert copied_data == test_data


class TestFileIOIntegrationDirectoryOperations:
    """Integration tests for directory operations."""
    
    def test_fmakedirs_creates_nested_directories(self, temp_dir):
        """Test that fmakedirs creates nested directory structures."""
        nested_path = os.path.join(temp_dir, "level1", "level2", "level3")
        
        # Create nested directories
        FileIOInterface.fmakedirs(path=nested_path)
        
        # Verify all levels were created
        assert os.path.exists(nested_path)
        assert os.path.isdir(nested_path)

    def test_fmakedirs_with_exist_ok_true_integration(self, temp_dir):
        """Test fmakedirs behavior when directories already exist."""
        test_dir = os.path.join(temp_dir, "existing_dir")
        
        # Create directory first time
        FileIOInterface.fmakedirs(path=test_dir, exist_ok=True)
        assert os.path.exists(test_dir)
        
        # Create same directory again (should not raise error)
        FileIOInterface.fmakedirs(path=test_dir, exist_ok=True)
        assert os.path.exists(test_dir)

    def test_fmakedirs_with_exist_ok_false_integration(self, temp_dir):
        """Test fmakedirs behavior with exist_ok=False."""
        test_dir = os.path.join(temp_dir, "test_exist_ok")
        
        # Create directory first time
        FileIOInterface.fmakedirs(path=test_dir, exist_ok=False)
        assert os.path.exists(test_dir)
        
        # Attempt to create same directory again should raise error
        with pytest.raises(OSError):
            FileIOInterface.fmakedirs(path=test_dir, exist_ok=False)

    def test_fdelete_removes_files_integration(self, temp_dir):
        """Test that fdelete removes files successfully."""
        test_file = os.path.join(temp_dir, "file_to_delete.txt")
        
        # Create file
        FileIOInterface.fwrite(write_path=test_file, data="Content to be deleted")
        assert os.path.exists(test_file)
        
        # Delete file
        FileIOInterface.fdelete(path=test_file)
        
        # Verify file was deleted
        assert not os.path.exists(test_file)

    def test_fdelete_removes_directories_integration(self, temp_dir):
        """Test that fdelete removes directories successfully."""
        test_dir = os.path.join(temp_dir, "dir_to_delete")
        
        # Create directory
        FileIOInterface.fmakedirs(path=test_dir)
        assert os.path.exists(test_dir)
        
        # Delete directory
        FileIOInterface.fdelete(path=test_dir)
        
        # Verify directory was deleted
        assert not os.path.exists(test_dir)


class TestFileIOIntegrationFileInfo:
    """Integration tests for file information operations."""
    
    def test_finfo_returns_file_information(self, existing_json_file):
        """Test that finfo returns actual file information."""
        file_info = FileIOInterface.finfo(fpath=existing_json_file)
        
        # Verify file info contains expected keys
        assert isinstance(file_info, dict)
        assert 'size' in file_info
        assert file_info['size'] > 0  # File should have content
        
        # Verify file size matches actual file
        actual_size = os.path.getsize(existing_json_file)
        assert file_info['size'] == actual_size

    def test_finfo_with_different_file_types(self, temp_dir, sample_dataframe, sample_text_data):
        """Test finfo works with different file types."""
        # Create files of different types
        files_data = [
            ("test.csv", sample_dataframe),
            ("test.txt", sample_text_data),
            ("test.json", {"test": "data"}),
        ]
        
        for filename, data in files_data:
            filepath = os.path.join(temp_dir, filename)
            FileIOInterface.fwrite(write_path=filepath, data=data)
            
            # Get file info
            file_info = FileIOInterface.finfo(fpath=filepath)
            
            # Verify basic file info
            assert isinstance(file_info, dict)
            assert 'size' in file_info
            assert file_info['size'] > 0


class TestFileIOIntegrationErrorHandling:
    """Integration tests for error handling with real file operations."""
    
    def test_fread_nonexistent_file_raises_error(self, temp_dir):
        """Test that reading non-existent file raises FileNotFoundError."""
        nonexistent_path = os.path.join(temp_dir, "does_not_exist.json")
        
        with pytest.raises(FileNotFoundError):
            FileIOInterface.fread(read_path=nonexistent_path)

    def test_finfo_nonexistent_file_raises_error(self, temp_dir):
        """Test that getting info for non-existent file raises OSError."""
        nonexistent_path = os.path.join(temp_dir, "does_not_exist.txt")
        
        with pytest.raises(OSError):
            FileIOInterface.finfo(fpath=nonexistent_path)

    def test_fcopy_nonexistent_source_raises_error(self, temp_dir):
        """Test that copying non-existent source file raises FileNotFoundError."""
        nonexistent_source = os.path.join(temp_dir, "nonexistent_source.txt")
        dest_path = os.path.join(temp_dir, "destination.txt")
        
        with pytest.raises(FileNotFoundError):
            FileIOInterface.fcopy(read_path=nonexistent_source, dest_path=dest_path)

    def test_fwrite_with_wrong_data_type_raises_error(self, temp_dir):
        """Test that writing wrong data type raises TypeError."""
        csv_path = os.path.join(temp_dir, "test.csv")
        
        # Try to write string to CSV file (requires DataFrame)
        with pytest.raises(TypeError, match="requires a pandas DataFrame"):
            FileIOInterface.fwrite(write_path=csv_path, data="not a dataframe")

    def test_unsupported_file_extension_raises_error(self, temp_dir):
        """Test that unsupported file extensions raise ValueError."""
        unsupported_path = os.path.join(temp_dir, "test.unsupported")
        
        with pytest.raises(ValueError, match="Unsupported file format"):
            FileIOInterface.fwrite(write_path=unsupported_path, data="test data")


class TestFileIOIntegrationEdgeCases:
    """Integration tests for edge cases and special scenarios."""
    
    def test_empty_file_handling(self, temp_dir):
        """Test handling of empty files."""
        empty_txt_path = os.path.join(temp_dir, "empty.txt")
        
        # Create empty text file
        FileIOInterface.fwrite(write_path=empty_txt_path, data="")
        
        # Read empty file
        content = FileIOInterface.fread(read_path=empty_txt_path)
        assert content == ""
        
        # Get info for empty file
        file_info = FileIOInterface.finfo(fpath=empty_txt_path)
        assert file_info['size'] == 0

    def test_large_data_handling(self, temp_dir):
        """Test handling of reasonably large data structures."""
        large_json_path = os.path.join(temp_dir, "large.json")
        
        # Create large data structure
        large_data = {
            'users': [
                {'id': i, 'name': f'User_{i}', 'data': [j for j in range(100)]}
                for i in range(100)
            ]
        }
        
        # Write and read large data
        FileIOInterface.fwrite(write_path=large_json_path, data=large_data)
        read_data = FileIOInterface.fread(read_path=large_json_path)
        
        # Verify large data roundtrip
        assert len(read_data['users']) == 100
        assert read_data['users'][0]['name'] == 'User_0'
        assert len(read_data['users'][0]['data']) == 100

    def test_unicode_content_handling(self, temp_dir):
        """Test handling of Unicode content in text files."""
        unicode_txt_path = os.path.join(temp_dir, "unicode.txt")
        
        # Text with various Unicode characters
        unicode_text = "Hello 世界! 🌍 Привет мир! 🚀 Testing émojis and açcénts"
        
        # Write and read Unicode text
        FileIOInterface.fwrite(write_path=unicode_txt_path, data=unicode_text)
        read_text = FileIOInterface.fread(read_path=unicode_txt_path)
        
        # Verify Unicode roundtrip
        assert read_text == unicode_text

    def test_special_characters_in_filenames(self, temp_dir):
        """Test handling of special characters in file names."""
        special_filename = "test file with spaces & symbols (1).json"
        special_path = os.path.join(temp_dir, special_filename)
        
        test_data = {"message": "File with special name"}
        
        # Write, read, and copy file with special name
        FileIOInterface.fwrite(write_path=special_path, data=test_data)
        read_data = FileIOInterface.fread(read_path=special_path)
        assert read_data == test_data
        
        # Test copying file with special name
        copy_path = os.path.join(temp_dir, "copy_" + special_filename)
        FileIOInterface.fcopy(read_path=special_path, dest_path=copy_path)
        
        copied_data = FileIOInterface.fread(read_path=copy_path)
        assert copied_data == test_data
