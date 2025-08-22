"""
Tests for duplicate file copy warnings in HDFS copy operations.
"""

import pytest
from unittest.mock import patch, MagicMock
from utilities import LogManager


class TestDuplicateFileWarnings:
    """Test warnings for duplicate files across multiple copy operations."""

    def test_no_warning_for_single_operation(self, log_manager, mock_file_discovery, capsys):
        """Test that no warning is issued when only one operation copies files."""
        files = ["/tmp/app.log", "/tmp/error.log"]
        mock_file_discovery.return_value = files
        
        log_manager._check_for_duplicate_files("copy1", files)
        
        captured = capsys.readouterr()
        assert "WARNING" not in captured.out

    def test_warning_for_overlapping_files(self, log_manager, mock_file_discovery, capsys):
        """Test warning format when multiple files overlap."""
        # Set up first operation
        first_files = ["/tmp/app.log", "/tmp/error.log", "/tmp/debug.log"]
        log_manager._copy_operations_files["copy1"] = set(first_files)

        # Second operation with multiple overlaps
        second_files = ["/tmp/app.log", "/tmp/error.log", "/tmp/unique.log"]
        log_manager._check_for_duplicate_files("copy2", second_files)

        captured = capsys.readouterr()
        assert "WARNING" in captured.out
        assert "copying 2 file(s)" in captured.out
        assert "/tmp/app.log" in captured.out
        assert "/tmp/error.log" in captured.out
        assert "/tmp/unique.log" not in captured.out  # Should not be mentioned in warning

    def test_no_warning_for_non_overlapping_files(self, log_manager, capsys):
        """Test that no warning is issued when files don't overlap."""
        # Set up first operation
        log_manager._copy_operations_files["copy1"] = {"/tmp/app.log", "/tmp/debug.log"}
        
        # Second operation with completely different files
        different_files = ["/tmp/other.log", "/tmp/new.log"]
        log_manager._check_for_duplicate_files("copy2", different_files)
        
        captured = capsys.readouterr()
        assert "WARNING" not in captured.out

    def test_multiple_operations_overlapping(self, log_manager, capsys):
        """Test warnings when multiple operations overlap with a new one."""
        # Set up multiple existing operations
        log_manager._copy_operations_files["copy1"] = {"/tmp/app.log", "/tmp/debug.log"}
        log_manager._copy_operations_files["copy2"] = {"/tmp/error.log", "/tmp/access.log"}
        
        # New operation overlapping with both
        third_files = ["/tmp/app.log", "/tmp/error.log", "/tmp/new.log"]
        log_manager._check_for_duplicate_files("copy3", third_files)

        captured = capsys.readouterr()
        warning_lines = [line for line in captured.out.split('\n') if "WARNING" in line]
        assert len(warning_lines) == 2  # One warning for each overlap

    def test_file_tracking_updates_correctly(self, log_manager):
        """Test that file tracking is updated after checking for duplicates."""
        files = ["/tmp/app.log", "/tmp/error.log"]
        
        # Initially no files tracked for this operation
        assert "copy1" not in log_manager._copy_operations_files
        
        log_manager._check_for_duplicate_files("copy1", files)
        
        # After checking, files should be tracked
        assert log_manager._copy_operations_files["copy1"] == set(files)

    def test_empty_file_list_handling(self, log_manager, capsys):
        """Test that empty file lists are handled gracefully."""
        log_manager._copy_operations_files["copy1"] = {"/tmp/app.log"}
        
        # Check with empty file list
        log_manager._check_for_duplicate_files("copy2", [])
        
        captured = capsys.readouterr()
        assert "WARNING" not in captured.out
        assert "copy2" not in log_manager._copy_operations_files

    @patch('utilities.LogManager.stop_all_hdfs_copy')
    @patch('builtins.print')
    def test_cleanup_removes_file_tracking(self, mock_print, mock_stop_all_hdfs_copy, log_manager):
        """Test that cleanup removes file tracking entries."""
        # Set up some file tracking
        log_manager._copy_operations_files["copy1"] = {"/tmp/app.log"}
        log_manager._copy_operations_files["copy2"] = {"/tmp/error.log"}
        
        # Mock trigger_hdfs_copy_now to prevent hanging during cleanup
        with patch.object(log_manager, 'trigger_hdfs_copy_now'):
            log_manager._cleanup()
        
        # File tracking should be cleared
        assert len(log_manager._copy_operations_files) == 0

