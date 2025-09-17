"""
Integration test suite for FileIO functionality.

These tests use real files and file system operations to validate
the complete FileIO workflow. Tests are designed to be Fast, Independent,
Repeatable, Self-Validating, and Timely (FIRST principles).
"""

import pytest
import os
import pandas as pd
import stat
import warnings
from pathlib import Path

from src.main.file_io import FileIOInterface

pytestmark = pytest.mark.integration


class TestFileIOIntegrationRoundtrip:
    """Integration tests for complete write-read cycles using parametrization."""
    
    def test_comprehensive_format_roundtrip_integration(self, file_path_by_extension, 
                                                       sample_data_by_extension, file_extension):
        """Test complete write-read cycle for ALL supported file formats.
        
        This test uses the comprehensive file_extension fixture that covers:
        - Text formats: txt, text, log, logs, sql (string data)
        - Serializable formats: json, yaml, yml (dict/list data)  
        - DataFrame formats: csv, parquet, arrow, feather (DataFrame data)
        - Pickle formats: pickle, pkl (any serializable data)
        """
        # Write data to file
        if file_extension == 'csv':
            # For CSV, write without index to ensure clean comparison
            FileIOInterface.fwrite(write_path=file_path_by_extension, data=sample_data_by_extension, index=False)
        else:
            FileIOInterface.fwrite(write_path=file_path_by_extension, data=sample_data_by_extension)
        
        # Verify file was created
        assert os.path.exists(file_path_by_extension)
        
        # Read data back from file
        read_data = FileIOInterface.fread(read_path=file_path_by_extension)
        
        # Verify data roundtrip is correct
        # Handle different data types and formats appropriately
        if file_extension in {'txt', 'text', 'log', 'logs', 'sql'}:
            # Text files: normalize line endings for cross-platform compatibility
            expected_data = sample_data_by_extension.replace('\n', '\r\n') if '\r\n' not in sample_data_by_extension else sample_data_by_extension
            assert read_data == expected_data
        elif file_extension in {'csv', 'parquet', 'arrow', 'feather'}:
            # DataFrame formats: compare DataFrames
            pd.testing.assert_frame_equal(read_data, sample_data_by_extension)
        else:
            # JSON, YAML, Pickle formats: direct comparison
            assert read_data == sample_data_by_extension

    @pytest.mark.parametrize("file_extension", ["json", "yaml", "txt"])
    def test_text_based_format_roundtrip_integration(self, file_path_by_extension, 
                                                   sample_data_by_extension, file_extension):
        """Test complete write-read cycle for text-based formats only.
        
        DEPRECATED: This test is kept for backward compatibility but the 
        comprehensive test above covers all formats more efficiently.
        """
        # Write data to file
        FileIOInterface.fwrite(write_path=file_path_by_extension, data=sample_data_by_extension)
        
        # Verify file was created
        assert os.path.exists(file_path_by_extension)
        
        # Read data back from file
        read_data = FileIOInterface.fread(read_path=file_path_by_extension)
        
        # Verify data roundtrip is correct
        # For text files, normalize line endings for cross-platform compatibility
        if file_extension == "txt":
            expected_data = sample_data_by_extension.replace('\n', '\r\n') if '\r\n' not in sample_data_by_extension else sample_data_by_extension
            assert read_data == expected_data
        else:
            assert read_data == sample_data_by_extension


class TestFileIOIntegrationOperations:
    """Integration tests for file operations."""
    
    def test_file_info_integration(self, existing_json_file):
        """Test file info retrieval with real files."""
        file_info = FileIOInterface.finfo(fpath=existing_json_file)
        
        # Verify file info contains expected keys
        assert 'size' in file_info
        assert file_info['size'] > 0  # File should have content
        
        # Verify file size matches actual file
        actual_size = os.path.getsize(existing_json_file)
        assert file_info['size'] == actual_size

    def test_file_copy_integration(self, existing_json_file, temp_dir):
        """Test file copying with real files."""
        dest_path = os.path.join(temp_dir, "copied_file.json")
        
        # Copy file
        FileIOInterface.fcopy(read_path=existing_json_file, dest_path=dest_path)
        
        # Verify destination file was created
        assert os.path.exists(dest_path)
        
        # Verify copied file has same content
        original_data = FileIOInterface.fread(read_path=existing_json_file)
        copied_data = FileIOInterface.fread(read_path=dest_path)
        assert original_data == copied_data

    def test_directory_creation_integration(self, temp_dir):
        """Test directory creation with real filesystem."""
        nested_dir = os.path.join(temp_dir, "level1", "level2", "level3")
        
        # Create nested directories
        FileIOInterface.fmakedirs(path=nested_dir)
        
        # Verify directory was created
        assert os.path.exists(nested_dir)
        assert os.path.isdir(nested_dir)

    def test_file_deletion_integration(self, temp_dir, sample_text_data):
        """Test file deletion with real files."""
        file_path = os.path.join(temp_dir, "to_delete.txt")
        
        # Create file first
        FileIOInterface.fwrite(write_path=file_path, data=sample_text_data)
        assert os.path.exists(file_path)
        
        # Delete file (use correct parameter name)
        FileIOInterface.fdelete(path=file_path)
        
        # Verify file was deleted
        assert not os.path.exists(file_path)


class TestFileIOIntegrationErrorHandling:
    """Integration tests for error handling scenarios."""
    
    def test_read_nonexistent_file_raises_error(self):
        """Test that reading non-existent file raises appropriate error."""
        with pytest.raises(FileNotFoundError):
            FileIOInterface.fread(read_path="/nonexistent/file.txt")

    def test_write_to_readonly_directory_handles_error(self, temp_dir):
        """Test error handling when writing to read-only directory."""
        
        # Make directory read-only (skip on Windows where this is more complex)
        try:
            os.chmod(temp_dir, stat.S_IRUSR | stat.S_IRGRP | stat.S_IROTH)
            readonly_file = os.path.join(temp_dir, "readonly_test.txt")
            
            # Should raise permission error
            with pytest.raises(PermissionError):
                FileIOInterface.fwrite(write_path=readonly_file, data="test")
                
        except (OSError, PermissionError):
            # Skip test if we can't make directory read-only
            pytest.skip("Cannot make directory read-only on this system")
        finally:
            # Restore permissions for cleanup
            try:
                os.chmod(temp_dir, stat.S_IRWXU | stat.S_IRWXG | stat.S_IRWXO)
            except (OSError, PermissionError):
                pass

    @pytest.mark.parametrize("invalid_extension", [".xyz", ".unknown", ""])
    def test_unsupported_file_format_raises_error(self, temp_dir, invalid_extension):
        """Test that unsupported file formats raise appropriate errors."""
        invalid_file = os.path.join(temp_dir, f"test{invalid_extension}")
        
        with pytest.raises(ValueError, match="Unsupported file format|has no extension"):
            FileIOInterface.fwrite(write_path=invalid_file, data="test")


class TestFileIOIntegrationDataTypes:
    """Integration tests for different data types and formats."""
    
    @pytest.mark.parametrize("data_and_extension", [
        ({"nested": {"data": ["list", "items"]}}, "json"),
        ({"database": {"host": "localhost", "port": 5432}}, "yaml"),
        ("Multi-line\ntext content\nwith special chars: àáâã", "txt"),
    ])
    def test_complex_data_structures_roundtrip(self, temp_dir, data_and_extension):
        """Test roundtrip with complex data structures."""
        data, extension = data_and_extension
        file_path = os.path.join(temp_dir, f"complex_test.{extension}")

        # Write and read back
        if extension == "txt":
            # Only pass encoding for text files
            FileIOInterface.fwrite(write_path=file_path, data=data, encoding='utf-8')
            read_data = FileIOInterface.fread(read_path=file_path, encoding='utf-8')
        else:
            # For JSON/YAML, don't pass encoding parameters
            FileIOInterface.fwrite(write_path=file_path, data=data)
            read_data = FileIOInterface.fread(read_path=file_path)

        # Verify data integrity
        if extension == "txt":
            # Handle line ending normalization for text files on Windows
            expected_data = data.replace('\n', '\r\n') if '\r\n' not in data else data
            assert read_data == expected_data
        else:
            assert read_data == data

    def test_dataframe_with_various_dtypes_roundtrip(self, temp_dir):
        """Test DataFrame with various data types survives CSV roundtrip."""
        import numpy as np
        
        complex_df = pd.DataFrame({
            'integers': [1, 2, 3, 4, 5],
            'floats': [1.1, 2.2, 3.3, 4.4, 5.5],
            'strings': ['a', 'b', 'c', 'd', 'e'],
            'booleans': [True, False, True, False, True],
            'dates': pd.date_range('2025-01-01', periods=5)
        })
        
        csv_path = os.path.join(temp_dir, "complex_dataframe.csv")
        
        # Write and read back (explicitly set index=False for CSV)
        FileIOInterface.fwrite(write_path=csv_path, data=complex_df, index=False)
        read_df = FileIOInterface.fread(read_path=csv_path)
        
        # Note: Some data types may change during CSV roundtrip
        # This is expected behavior, so we check shape and basic content
        assert read_df.shape == complex_df.shape
        assert list(read_df.columns) == list(complex_df.columns)

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
        # We expect a warning to be issued before the OSError is raised
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", UserWarning)
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
        
        # We expect a warning to be issued before the OSError is raised
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", UserWarning)
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
        
        # Write and read Unicode text with explicit UTF-8 encoding
        FileIOInterface.fwrite(write_path=unicode_txt_path, data=unicode_text, encoding='utf-8')
        read_text = FileIOInterface.fread(read_path=unicode_txt_path, encoding='utf-8')
        
        # Verify Unicode roundtrip (handle potential line ending differences)
        expected_text = unicode_text.replace('\n', '\r\n') if '\r\n' not in unicode_text else unicode_text
        assert read_text == expected_text

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


class TestFileIOIntegrationDataTypeValidation:
    """Test that file extensions only accept appropriate data types."""
    
    def test_data_type_validation_for_file_extensions(self, temp_dir, sample_dataframe, 
                                                    sample_text_data, sample_json_data):
        """Test that each file format enforces correct data types.
        
        This validates the data type enforcement described in _base.py:
        - DataFrame formats: csv, parquet, arrow, feather (require DataFrame)
        - Text formats: txt, text, log, logs, sql (require string)  
        - Serializable formats: json, yaml, yml, pickle, pkl (flexible)
        """
        # Test DataFrame formats require DataFrame
        dataframe_extensions = ['csv', 'parquet', 'arrow', 'feather']
        for ext in dataframe_extensions:
            file_path = str(Path(temp_dir) / f"test.{ext}")
            
            # Should work with DataFrame
            FileIOInterface.fwrite(write_path=file_path, data=sample_dataframe)
            assert os.path.exists(file_path)
            
            # Should fail with string data
            with pytest.raises(TypeError, match=f"Writing {ext.upper()} files requires a pandas DataFrame"):
                FileIOInterface.fwrite(write_path=file_path, data=sample_text_data)
        
        # Test text formats require string
        text_extensions = ['txt', 'text', 'log', 'logs', 'sql']
        for ext in text_extensions:
            file_path = str(Path(temp_dir) / f"test.{ext}")
            
            # Should work with string
            FileIOInterface.fwrite(write_path=file_path, data=sample_text_data)
            assert os.path.exists(file_path)
            
            # Should fail with DataFrame
            with pytest.raises(TypeError, match=f"Writing {ext.upper()} files requires a string"):
                FileIOInterface.fwrite(write_path=file_path, data=sample_dataframe)
        
        # Test serializable formats are flexible (should accept various types)
        serializable_extensions = ['json', 'yaml', 'yml', 'pickle', 'pkl']
        for ext in serializable_extensions:
            file_path = str(Path(temp_dir) / f"test.{ext}")
            
            # Should work with dict data
            FileIOInterface.fwrite(write_path=file_path, data=sample_json_data)
            assert os.path.exists(file_path)
            
            # Note: JSON/YAML may have restrictions on certain data types
            # but pickle should accept almost anything


class TestFileIOIntegrationComprehensiveRoundtrip:
    """Comprehensive roundtrip tests for all supported file formats."""
    
    def test_all_extensions_roundtrip_with_appropriate_data(self, file_extension, temp_dir):
        """Test roundtrip for every supported extension with format-appropriate data.
        
        This test creates specific test data for each format and verifies complete
        write-read cycles work correctly.
        """
        import pickle
        
        file_path = str(Path(temp_dir) / f"comprehensive_test.{file_extension}")
        
        # Create format-specific test data
        if file_extension in {'txt', 'text', 'log', 'logs', 'sql'}:
            test_data = f"Test content for {file_extension} format\nLine 2\nLine 3"
            expected_data = test_data.replace('\n', '\r\n') if '\r\n' not in test_data else test_data
        elif file_extension in {'csv', 'parquet', 'arrow', 'feather'}:
            test_data = pd.DataFrame({
                'id': [1, 2, 3],
                'name': ['Test1', 'Test2', 'Test3'],
                'value': [10.5, 20.3, 30.7]
            })
            expected_data = test_data
        elif file_extension in {'json'}:
            test_data = {
                'test_key': 'test_value',
                'numbers': [1, 2, 3],
                'nested': {'inner_key': 'inner_value'}
            }
            expected_data = test_data
        elif file_extension in {'yaml', 'yml'}:
            test_data = {
                'config': {
                    'database': 'test_db',
                    'timeout': 30
                },
                'features': ['feature1', 'feature2']
            }
            expected_data = test_data
        elif file_extension in {'pickle', 'pkl'}:
            test_data = {
                'complex_data': [1, 2, {'nested': True}],
                'tuple_data': (1, 2, 3)
            }
            expected_data = test_data
        else:
            pytest.fail(f"Unsupported extension in test: {file_extension}")
        
        # Write data
        if file_extension == 'csv':
            # For CSV, write without index to ensure clean comparison
            FileIOInterface.fwrite(write_path=file_path, data=test_data, index=False)
        else:
            FileIOInterface.fwrite(write_path=file_path, data=test_data)
        assert os.path.exists(file_path)
        
        # Read data back
        read_data = FileIOInterface.fread(read_path=file_path)
        
        # Verify data integrity
        if file_extension in {'csv', 'parquet', 'arrow', 'feather'}:
            pd.testing.assert_frame_equal(read_data, expected_data)
        elif file_extension in {'txt', 'text', 'log', 'logs', 'sql'}:
            assert read_data == expected_data
        else:
            assert read_data == expected_data
