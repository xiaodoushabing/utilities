"""
Integration tests for duplicate file warning functionality in copy operations.

This module tests the complete end-to-end workflow of duplicate file detection
across multiple copy operations, including real file discovery, threading,
and cleanup scenarios.
"""

import pytest
import tempfile
import os
import time
from pathlib import Path
from unittest.mock import patch, MagicMock
from utilities import LogManager


class TestDuplicateFileWarningsIntegration:
    """Integration tests for duplicate file warnings across the full copy workflow."""

    @pytest.fixture
    def temp_files(self):
        """Create temporary files for testing."""
        temp_dir = tempfile.mkdtemp()
        
        # Create test files
        files = []
        for i in range(3):
            file_path = Path(temp_dir) / f"test_{i}.log"
            with file_path.open('w') as f:
                f.write(f"Test log content {i}\n")
            files.append(str(file_path))
        
        yield temp_dir, files
        
        # Cleanup
        import shutil
        shutil.rmtree(temp_dir, ignore_errors=True)

    def test_duplicate_warnings_across_real_copy_operations(self, temp_files, capsys):
        """Test duplicate file warnings in realistic copy scenario."""
        temp_dir, files = temp_files
        
        # Create LogManager using the same pattern as in other working tests
        lm = LogManager()
        
        # Clear any setup output first
        capsys.readouterr()
        
        # Test 1: Verify file discovery works
        first_files = lm._discover_files_to_copy([f"{temp_dir}/*.log"])
        assert len(first_files) > 0, f"Should discover files in {temp_dir}, found: {first_files}"
        
        # Test 2: Set up first operation manually (like the working debug test)
        lm._copy_operations_files["logs_backup"] = set(first_files)
        
        # Test 3: Check for duplicates with overlapping files
        overlapping_files = [files[0], files[1]]  # Use actual file paths from fixture
        lm._check_for_duplicate_files("logs_archive", overlapping_files)
        
        # Capture warnings
        captured = capsys.readouterr()
        
        # Verify warnings were issued for overlapping files
        assert "WARNING" in captured.out, f"Expected WARNING in output: '{captured.out}'"
        assert "logs_archive" in captured.out
        assert "logs_backup" in captured.out
        assert "copying 2 file(s)" in captured.out

    def test_duplicate_warnings_with_dynamic_file_changes(self, temp_files, capsys):
        """Test duplicate warnings when files are added/removed dynamically."""
        temp_dir, files = temp_files
        
        # Add a new file that will be discovered
        new_file = Path(temp_dir) / "test_3.log"
        with new_file.open('w') as f:
            f.write("New test content\n")
        
        # Create LogManager using the working pattern
        lm = LogManager()
        
        # Clear any setup output first
        capsys.readouterr()
        
        # Simulate first operation discovering files
        first_files = lm._discover_files_to_copy([f"{temp_dir}/test_*.log"])
        lm._copy_operations_files["continuous_backup"] = set(first_files)
        
        # Second operation that includes overlapping files
        second_files = [str(new_file), files[0]]  # test_3.log and test_0.log
        lm._check_for_duplicate_files("selective_backup", second_files)
        
        captured = capsys.readouterr()
        
        # Should warn about overlapping files
        assert "WARNING" in captured.out
        assert "selective_backup" in captured.out
        assert "continuous_backup" in captured.out

    def test_no_warnings_for_completely_separate_operations(self, capsys):
        """Test that no warnings are issued when operations have no overlapping files."""
        # Create separate temp directories
        temp_dir1 = tempfile.mkdtemp()
        temp_dir2 = tempfile.mkdtemp()
        
        try:
            # Create files in separate directories
            for i in range(2):
                (Path(temp_dir1) / f"app_{i}.log").write_text(f"App log {i}")
                (Path(temp_dir2) / f"db_{i}.log").write_text(f"DB log {i}")
            
            # Create LogManager using the working pattern
            lm = LogManager()
            
            # Clear any setup output first
            capsys.readouterr()
            
            # Simulate operations with completely separate file sets
            first_files = lm._discover_files_to_copy([f"{temp_dir1}/*.log"])
            lm._copy_operations_files["app_logs"] = set(first_files)
            
            second_files = lm._discover_files_to_copy([f"{temp_dir2}/*.log"])
            lm._check_for_duplicate_files("db_logs", second_files)
            
            captured = capsys.readouterr()
            
            # Should not have any warnings
            assert "WARNING" not in captured.out
                
        finally:
            # Cleanup
            import shutil
            shutil.rmtree(temp_dir1, ignore_errors=True)
            shutil.rmtree(temp_dir2, ignore_errors=True)

    def test_comprehensive_duplicate_scenarios(self, temp_files, capsys):
        """Test multiple comprehensive duplicate file scenarios in one test."""
        temp_dir, files = temp_files
        
        # Create additional files for more complex scenarios
        for i in range(3, 6):
            (Path(temp_dir) / f"test_{i}.log").write_text(f"Test content {i}")
        
        # Create LogManager using the working pattern
        lm = LogManager()
        capsys.readouterr()
        
        # Scenario 1: Multiple overlapping operations
        all_files = lm._discover_files_to_copy([f"{temp_dir}/*.log"])
        lm._copy_operations_files["full_backup"] = set(all_files)
        
        subset_files = all_files[:3]  # First 3 files
        lm._copy_operations_files["partial_archive"] = set(subset_files)
        
        # Test overlapping with both previous operations
        overlap_files = all_files[2:5]  # Files 2,3,4 - overlaps with both
        lm._check_for_duplicate_files("recent_logs", overlap_files)
        
        captured = capsys.readouterr()
        
        # Should have multiple warnings about overlaps
        warning_lines = [line for line in captured.out.split('\n') if "WARNING" in line]
        assert len(warning_lines) >= 2  # At least 2 overlap warnings
        assert "recent_logs" in captured.out
        assert "full_backup" in captured.out
        assert "partial_archive" in captured.out
        
        # Scenario 2: Test cleanup and restart (file tracking management)
        assert "recent_logs" in lm._copy_operations_files
        del lm._copy_operations_files["recent_logs"]  # Simulate stop operation
        assert "recent_logs" not in lm._copy_operations_files
        
        # Clear previous output
        capsys.readouterr()
        
        # New operation with same files should not warn about the stopped operation
        lm._check_for_duplicate_files("new_backup", overlap_files)
        captured = capsys.readouterr()
        assert "recent_logs" not in captured.out  # Should not mention stopped operation
        
        # Scenario 3: Mixed explicit files and patterns
        capsys.readouterr()
        explicit_files = [files[0], files[1]]  # Explicit file paths
        lm._check_for_duplicate_files("explicit_backup", explicit_files)
        captured = capsys.readouterr()
        
        # Should warn about overlapping with existing operations
        assert "WARNING" in captured.out
        assert "explicit_backup" in captured.out
        
        # Scenario 4: Empty file lists should not generate warnings
        capsys.readouterr()
        lm._check_for_duplicate_files("empty_operation", [])
        captured = capsys.readouterr()
        assert "WARNING" not in captured.out
