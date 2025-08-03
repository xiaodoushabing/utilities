"""
Integration tests for FileIO operations.

This test suite focuses on real-world scenarios and end-to-end testing:
- Multi-format data pipelines
- File system integration
- Error recovery scenarios
- Performance considerations

Key integration testing patterns:
📚 Real file operations: Using actual filesystem instead of mocks
🔄 Multi-step workflows: Testing complete data processing pipelines
🎭 Error scenarios: Testing recovery from filesystem errors
🧪 Cross-format operations: Testing data conversion between formats
"""

import pytest
import os
import json
import yaml
import pandas as pd
import time
from pathlib import Path
from unittest.mock import patch, MagicMock

from src.main.file_io import FileIOInterface

pytestmark = [pytest.mark.integration, pytest.mark.fileio]


class TestFileIOIntegration:
    """Integration tests for FileIO operations with real files."""
    
    def test_complete_data_processing_pipeline(self, temp_directory, sample_datasets):
        """Test a complete data processing pipeline across multiple formats."""
        # Step 1: Create input configuration (JSON)
        config = {
            "input_file": "data.csv",
            "output_format": "json",
            "processing": {
                "filter_age_above": 25,
                "sort_by": "name"
            }
        }
        config_path = os.path.join(temp_directory, "config.json")
        FileIOInterface.fwrite(config_path, config)
        
        # Step 2: Create input data (CSV)
        input_data = pd.DataFrame({
            "name": ["Alice", "Bob", "Charlie", "David", "Eve"],
            "age": [23, 28, 31, 19, 35],
            "department": ["IT", "HR", "IT", "Finance", "IT"],
            "salary": [50000, 60000, 70000, 45000, 80000]
        })
        data_path = os.path.join(temp_directory, "data.csv")
        FileIOInterface.fwrite(data_path, input_data)
        
        # Step 3: Process data according to configuration
        loaded_config = FileIOInterface.fread(config_path)
        loaded_data = FileIOInterface.fread(data_path)
        
        # Apply filtering and sorting
        filtered_data = loaded_data[loaded_data["age"] > loaded_config["processing"]["filter_age_above"]]
        sorted_data = filtered_data.sort_values(by=loaded_config["processing"]["sort_by"])
        
        # Step 4: Convert to output format and save
        output_data = sorted_data.to_dict("records")
        output_path = os.path.join(temp_directory, "processed_data.json")
        FileIOInterface.fwrite(output_path, output_data)
        
        # Step 5: Verify pipeline results
        result = FileIOInterface.fread(output_path)
        
        assert isinstance(result, list)
        assert len(result) == 3  # Should have 3 people above age 25
        assert result[0]["name"] == "Alice"  # Sorted alphabetically
        assert result[1]["name"] == "Bob"
        assert result[2]["name"] == "Charlie"
        assert all(person["age"] > 25 for person in result)
    
    def test_multi_format_data_conversion(self, temp_directory):
        """Test converting data between different formats."""
        # Original data
        original_data = {
            "project": "FileIO Testing",
            "version": "1.0.0",
            "team": ["Alice", "Bob", "Charlie"],
            "config": {
                "debug": True,
                "timeout": 30
            }
        }
        
        # Step 1: JSON -> YAML
        json_path = os.path.join(temp_directory, "original.json")
        FileIOInterface.fwrite(json_path, original_data)
        
        loaded_from_json = FileIOInterface.fread(json_path)
        
        yaml_path = os.path.join(temp_directory, "converted.yaml")
        FileIOInterface.fwrite(yaml_path, loaded_from_json)
        
        # Step 2: YAML -> JSON (round trip)
        loaded_from_yaml = FileIOInterface.fread(yaml_path)
        
        json_roundtrip_path = os.path.join(temp_directory, "roundtrip.json")
        FileIOInterface.fwrite(json_roundtrip_path, loaded_from_yaml)
        
        final_data = FileIOInterface.fread(json_roundtrip_path)
        
        # Verify data integrity through format conversions
        assert final_data == original_data
    
    def test_backup_and_versioning_workflow(self, temp_directory):
        """Test a backup and versioning workflow."""
        # Create original data files
        files_to_backup = {
            "config.json": {"app": "test", "version": "1.0"},
            "data.txt": "Important application data\nLine 2\nLine 3",
            "settings.yaml": {"debug": False, "port": 8080}
        }
        
        # Create backup directory structure
        backup_dir = os.path.join(temp_directory, "backups", "v1.0")
        FileIOInterface.fmakedirs(backup_dir)
        
        original_paths = {}
        backup_paths = {}
        
        # Create original files
        for filename, data in files_to_backup.items():
            original_path = os.path.join(temp_directory, filename)
            FileIOInterface.fwrite(original_path, data)
            original_paths[filename] = original_path
            
            # Create backup copy
            backup_path = os.path.join(backup_dir, filename)
            FileIOInterface.fcopy(original_path, backup_path)
            backup_paths[filename] = backup_path
        
        # Simulate data changes
        updated_data = {
            "config.json": {"app": "test", "version": "2.0", "new_feature": True},
            "data.txt": "Updated application data\nNew line\nModified content",
            "settings.yaml": {"debug": True, "port": 9090, "ssl": True}
        }
        
        # Update original files
        for filename, data in updated_data.items():
            FileIOInterface.fwrite(original_paths[filename], data)
        
        # Verify backups preserve original data
        for filename, original_data in files_to_backup.items():
            backup_content = FileIOInterface.fread(backup_paths[filename])
            assert backup_content == original_data
        
        # Verify originals have updated data
        for filename, new_data in updated_data.items():
            updated_content = FileIOInterface.fread(original_paths[filename])
            assert updated_content == new_data
    
    def test_directory_operations_and_file_management(self, temp_directory):
        """Test directory creation and file management operations."""
        # Create nested directory structure
        project_structure = {
            "src/main/python": "main.py",
            "src/test/python": "test_main.py", 
            "config/dev": "dev_config.json",
            "config/prod": "prod_config.json",
            "data/input": "input_data.csv",
            "data/output": "results.json",
            "docs": "README.md"
        }
        
        created_files = []
        
        # Create directory structure and files
        for dir_path, filename in project_structure.items():
            full_dir = os.path.join(temp_directory, dir_path)
            FileIOInterface.fmakedirs(full_dir)
            
            file_path = os.path.join(full_dir, filename)
            FileIOInterface.fwrite(file_path, f"Content for {filename}")
            created_files.append(file_path)
        
        # Verify all directories and files exist
        for file_path in created_files:
            assert os.path.exists(file_path)
            assert os.path.isfile(file_path)
        
        # Test file information gathering
        for file_path in created_files:
            file_info = FileIOInterface.finfo(file_path)
            assert isinstance(file_info, dict)
            assert "size" in file_info or "name" in file_info
        
        # Clean up - delete all files
        for file_path in created_files:
            FileIOInterface.fdelete(file_path)
            assert not os.path.exists(file_path)
    
    def test_large_file_handling(self, temp_directory):
        """Test handling of larger files (relative to test environment)."""
        # Create a moderately large text file
        large_content_lines = []
        for i in range(1000):
            large_content_lines.append(f"Line {i:04d}: This is test data with some content to make it longer.")
        
        large_content = "\n".join(large_content_lines)
        large_file_path = os.path.join(temp_directory, "large_file.txt")
        
        # Write large file
        FileIOInterface.fwrite(large_file_path, large_content)
        
        # Verify file exists and has expected size
        assert os.path.exists(large_file_path)
        file_info = FileIOInterface.finfo(large_file_path)
        assert file_info["size"] > 50000  # Should be reasonably large
        
        # Read large file back
        read_content = FileIOInterface.fread(large_file_path)
        assert read_content == large_content
        
        # Test copying large file
        copy_path = os.path.join(temp_directory, "large_file_copy.txt")
        FileIOInterface.fcopy(large_file_path, copy_path)
        
        # Verify copy is identical
        copy_content = FileIOInterface.fread(copy_path)
        assert copy_content == large_content
    
    def test_concurrent_file_operations(self, temp_directory):
        """Test multiple file operations in sequence (simulating concurrent usage)."""
        
        # Create multiple files rapidly
        file_data = {}
        for i in range(10):
            filename = f"file_{i:02d}.json"
            data = {"file_id": i, "timestamp": time.time(), "data": f"content_{i}"}
            file_path = os.path.join(temp_directory, filename)
            FileIOInterface.fwrite(file_path, data)
            file_data[filename] = data
        
        # Read all files back and verify
        for filename, expected_data in file_data.items():
            file_path = os.path.join(temp_directory, filename)
            read_data = FileIOInterface.fread(file_path)
            assert read_data == expected_data
        
        # Test batch operations
        all_data = {}
        for filename in file_data.keys():
            file_path = os.path.join(temp_directory, filename)
            all_data[filename] = FileIOInterface.fread(file_path)
        
        # Write consolidated data
        consolidated_path = os.path.join(temp_directory, "consolidated.json")
        FileIOInterface.fwrite(consolidated_path, all_data)
        
        # Verify consolidated data
        consolidated_data = FileIOInterface.fread(consolidated_path)
        assert len(consolidated_data) == 10
        for filename, data in file_data.items():
            assert consolidated_data[filename] == data


class TestErrorRecoveryScenarios:
    """Test error recovery and robustness scenarios."""
    
    def test_recovery_from_corrupted_files(self, temp_directory, corrupted_files):
        """Test graceful handling of corrupted files."""
        # Try to read corrupted files and handle errors appropriately
        error_log = []
        
        for file_type, file_path in corrupted_files.items():
            try:
                FileIOInterface.fread(file_path)
                # If no error, log that it succeeded unexpectedly
                error_log.append((file_type, None, "No error raised"))
            except Exception as e:
                error_log.append((file_type, type(e).__name__, str(e)))
        
        # Verify we got expected errors
        assert len(error_log) > 0
        
        # Check that specific error types were raised for specific corruptions
        error_types = {file_type: error_type for file_type, error_type, _ in error_log}
        
        if "json_corrupted" in error_types:
            assert error_types["json_corrupted"] in ["JSONDecodeError", "ValueError"]
        if "yaml_corrupted" in error_types:
            assert error_types["yaml_corrupted"] in ["YAMLError", "ParserError", "ScannerError"]
        if "binary_text" in error_types:
            assert error_types["binary_text"] in ["UnicodeDecodeError", "UnicodeError"]
    
    def test_disk_space_simulation(self, temp_directory):
        """Test behavior when approaching disk space limits (simulated)."""
        # We can't actually fill up disk space in tests, but we can test
        # the error handling path by mocking filesystem errors
        
        test_data = {"test": "data"}
        test_file = os.path.join(temp_directory, "diskfull_test.json")
        
        # Mock filesystem error during write
        with patch('src.main.file_io.FileIOInterface._instantiate') as mock_instantiate:
            mock_fileio = MagicMock()
            mock_fileio._fwrite.side_effect = OSError("No space left on device")
            mock_instantiate.return_value = mock_fileio
            
            with pytest.raises(OSError, match="No space left on device"):
                FileIOInterface.fwrite(test_file, test_data)
    
    def test_permission_error_handling(self, temp_directory):
        """Test handling of permission errors."""
        test_file = os.path.join(temp_directory, "readonly_test.txt")
        test_data = "test content"
        
        # Create the file first
        FileIOInterface.fwrite(test_file, test_data)
        
        # On Windows, permission testing is more complex, so we'll mock it
        with patch('src.main.file_io.FileIOInterface._instantiate') as mock_instantiate:
            mock_fileio = MagicMock()
            mock_fileio._fwrite.side_effect = PermissionError("Permission denied")
            mock_instantiate.return_value = mock_fileio
            
            with pytest.raises(PermissionError, match="Permission denied"):
                FileIOInterface.fwrite(test_file, "new content")
    
    def test_network_filesystem_simulation(self, temp_directory):
        """Test behavior with network filesystem issues (simulated)."""
        test_file = os.path.join(temp_directory, "network_test.json")
        test_data = {"network": "test"}
        
        # Simulate network timeout during write
        with patch('src.main.file_io.FileIOInterface._instantiate') as mock_instantiate:
            mock_fileio = MagicMock()
            mock_fileio._fwrite.side_effect = TimeoutError("Network timeout")
            mock_instantiate.return_value = mock_fileio
            
            with pytest.raises(TimeoutError, match="Network timeout"):
                FileIOInterface.fwrite(test_file, test_data)


class TestPerformanceConsiderations:
    """Tests focused on performance aspects of FileIO operations."""
    
    def test_file_operation_timing(self, temp_directory):
        """Test that file operations complete within reasonable time."""
        # Create test data
        test_data = {"data": list(range(1000))}
        test_file = os.path.join(temp_directory, "timing_test.json")
        
        # Measure write time
        start_time = time.time()
        FileIOInterface.fwrite(test_file, test_data)
        write_time = time.time() - start_time
        
        # Measure read time
        start_time = time.time()
        read_data = FileIOInterface.fread(test_file)
        read_time = time.time() - start_time
        
        # Verify data integrity
        assert read_data == test_data
        
        # Reasonable performance expectations (adjust based on test environment)
        assert write_time < 1.0  # Should complete within 1 second
        assert read_time < 1.0   # Should complete within 1 second
    
    def test_memory_efficiency_with_large_data(self, temp_directory):
        """Test memory efficiency with larger datasets."""
        # Create a reasonably large DataFrame
        large_df = pd.DataFrame({
            'id': range(10000),
            'value': [f"value_{i}" for i in range(10000)],
            'category': ['A', 'B', 'C'] * 3334  # Repeating pattern
        })
        
        csv_file = os.path.join(temp_directory, "large_data.csv")
        
        # Write large DataFrame
        FileIOInterface.fwrite(csv_file, large_df)
        
        # Read it back
        read_df = FileIOInterface.fread(csv_file)
        
        # Verify data integrity
        assert len(read_df) == len(large_df)
        assert list(read_df.columns) == list(large_df.columns)
        
        # Basic data spot checks
        assert read_df.iloc[0]['id'] == 0
        assert read_df.iloc[-1]['id'] == 9999
        assert read_df.iloc[5000]['value'] == "value_5000"


# ========================================================================================
# PYTEST LEARNING TIP 💡
# Integration tests are valuable for:
# 1. Testing real-world scenarios that unit tests can't cover
# 2. Verifying that components work together correctly
# 3. Catching performance regressions
# 4. Testing error recovery and robustness
# 
# However, they're also:
# - Slower to run than unit tests
# - More complex to debug when they fail
# - More dependent on the test environment
# 
# Use them strategically to complement, not replace, unit tests.
# ========================================================================================
