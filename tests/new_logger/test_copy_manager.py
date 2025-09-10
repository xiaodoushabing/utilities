"""
Test suite for CopyManager class.

This file contains all tests related to the CopyManager component,
which handles background file copying operations and thread management.
"""

import pytest
import threading
import time
from pathlib import Path
from unittest.mock import patch, MagicMock, call

from src.main.logger import CopyManager

pytestmark = pytest.mark.unit


class TestCopyManagerInstantiation:
    """Test CopyManager initialization and basic properties."""
    
    def test_initialization_with_defaults(self):
        """Test CopyManager initialization with default parameters."""
        manager = CopyManager()
        assert manager._enabled is True
        assert isinstance(manager._copy_threads, dict)
        assert isinstance(manager._stop_events, dict)
        assert isinstance(manager._copy_operations_files, dict)
        assert isinstance(manager._copy_operations_params, dict)
        assert manager._shutdown_in_progress is False

    def test_initialization_disabled(self):
        """Test CopyManager initialization when disabled."""
        manager = CopyManager(enabled=False)
        assert manager._enabled is False

    def test_initialization_with_config(self):
        """Test CopyManager initialization with configuration."""
        config = {"some_key": "some_value"}
        manager = CopyManager(config=config)
        assert manager.config == config

    def test_enabled_attribute(self):
        """Test _enabled attribute returns correct state."""
        enabled_manager = CopyManager(enabled=True)
        assert enabled_manager._enabled is True
        
        disabled_manager = CopyManager(enabled=False)
        assert disabled_manager._enabled is False


class TestCopyValidation:
    """Test copy parameter validation."""
    
    @pytest.fixture
    def copy_manager(self):
        """Create a CopyManager instance for testing."""
        manager = CopyManager(enabled=True)
        yield manager
        manager.cleanup()

    @pytest.mark.parametrize("invalid_params", [
        {"copy_name": ""},
        {"copy_name": None},
        {"path_patterns": []},
        {"path_patterns": None},
        {"copy_destination": ""},
        {"copy_destination": None},
        {"copy_interval": 0},
        {"copy_interval": -1},
        {"max_retries": -1},
        {"retry_delay": -1},
        {"preserve_structure": True, "root_dir": None},
    ])
    def test_invalid_parameters_raise_error(self, copy_manager, copy_defaults, invalid_params):
        """Test that invalid parameters raise ValueError."""
        params = {**copy_defaults, **invalid_params}
        
        with pytest.raises(ValueError):
            copy_manager.start_copy(**params)

    def test_duplicate_copy_name_rejected(self, copy_manager, copy_defaults, mock_thread, mock_event):
        """Test that duplicate copy names are rejected."""
        # First call should succeed
        copy_manager.start_copy(**copy_defaults)
        
        # Second call with same name should fail
        with pytest.raises(ValueError, match="already exists"):
            copy_manager.start_copy(**copy_defaults)

    def test_cannot_start_copy_when_disabled(self, copy_defaults, capsys):
        """Test that copy operations are skipped when disabled."""
        disabled_manager = CopyManager(enabled=False)
        
        # Should not raise an error, but should skip and print message
        disabled_manager.start_copy(**copy_defaults)
        
        captured = capsys.readouterr()
        assert "skipped: disabled in system environment" in captured.out

    def test_cannot_start_copy_during_shutdown(self, copy_manager, copy_defaults):
        """Test that copy operations cannot be started during shutdown."""
        copy_manager._shutdown_in_progress = True
        
        with pytest.raises(ValueError, match="shutting down"):
            copy_manager.start_copy(**copy_defaults)


class TestCopyOperations:
    """Test copy operation management."""
    
    @pytest.fixture
    def copy_manager(self):
        """Create a CopyManager instance for testing."""
        manager = CopyManager(enabled=True)
        yield manager
        manager.cleanup()

    def test_start_copy_creates_thread(self, copy_manager, copy_defaults, mock_thread, mock_event):
        """Test that starting a copy operation creates a thread."""
        copy_manager.start_copy(**copy_defaults)
        
        # Verify thread was created
        mock_thread.assert_called_once()
        assert copy_defaults["copy_name"] in copy_manager._copy_threads

    def test_start_copy_from_config(self, copy_manager, mock_thread, mock_event):
        """Test starting copy operations from configuration."""
        config = {
            "op1": {
                "path_patterns": ["/tmp/*.log"],
                "copy_destination": "hdfs://dest1/",
                "copy_interval": 30
            },
            "op2": {
                "path_patterns": ["/var/log/*.log"],
                "copy_destination": "hdfs://dest2/",
                "copy_interval": 60
            }
        }
        
        copy_manager.start_copy_from_config(config)
        
        # Should have created threads for both operations
        assert len(copy_manager._copy_threads) == 2
        assert "op1" in copy_manager._copy_threads
        assert "op2" in copy_manager._copy_threads

    def test_stop_copy_operation(self, copy_manager, copy_defaults, mock_thread, mock_event):
        """Test stopping a specific copy operation."""
        # Start a copy operation
        copy_manager.start_copy(**copy_defaults)
        
        # Stop it
        result = copy_manager.stop_copy(copy_defaults["copy_name"])
        
        assert result is True
        # Thread should be removed from tracking
        assert copy_defaults["copy_name"] not in copy_manager._copy_threads

    def test_stop_nonexistent_copy_raises_error(self, copy_manager):
        """Test that stopping a non-existent copy operation raises ValueError."""
        with pytest.raises(ValueError, match="does not exist"):
            copy_manager.stop_copy("nonexistent")

    def test_stop_all_copy_operations(self, copy_manager, mock_thread, mock_event):
        """Test stopping all copy operations."""
        # Start multiple operations
        for i in range(3):
            params = {
                "copy_name": f"test_{i}",
                "path_patterns": [f"/tmp/test_{i}*.log"],
                "copy_destination": "hdfs://dest/",
                "copy_interval": 60
            }
            copy_manager.start_copy(**params)
        
        # Stop all operations
        failed_operations = copy_manager.stop_all_copy_operations()
        
        # All should have stopped successfully
        assert len(failed_operations) == 0
        assert len(copy_manager._copy_threads) == 0

    def test_list_copy_operations(self, copy_manager, copy_defaults, mock_thread, mock_event):
        """Test listing active copy operations."""
        # Start a copy operation
        copy_manager.start_copy(**copy_defaults)
        
        operations = copy_manager.list_copy_operations()
        
        assert len(operations) == 1
        assert operations[0]["name"] == copy_defaults["copy_name"]

    def test_trigger_copy_now(self, copy_manager, copy_defaults, mock_thread, mock_event):
        """Test triggering immediate copy."""
        # Start a copy operation
        copy_manager.start_copy(**copy_defaults)
        
        # Should not raise any errors - pass as list
        copy_manager.trigger_copy_now([copy_defaults["copy_name"]])

    def test_trigger_copy_now_all_operations(self, copy_manager, mock_thread, mock_event):
        """Test triggering immediate copy for all operations."""
        # Start multiple operations
        for i in range(2):
            params = {
                "copy_name": f"test_{i}",
                "path_patterns": [f"/tmp/test_{i}*.log"],
                "copy_destination": "hdfs://dest/",
                "copy_interval": 60
            }
            copy_manager.start_copy(**params)
        
        # Should not raise any errors
        copy_manager.trigger_copy_now()


class TestDuplicateFileWarnings:
    """Test warnings for duplicate files across multiple copy operations."""
    
    @pytest.fixture
    def copy_manager(self):
        """Create a CopyManager instance for testing."""
        manager = CopyManager(enabled=True)
        yield manager
        manager.cleanup()

    def test_no_warning_for_single_operation(self, copy_manager, capsys):
        """Test that no warning is issued when only one operation copies files."""
        files = ["/tmp/app.log", "/tmp/error.log"]
        
        copy_manager._check_for_duplicate_files("copy1", files)
        
        captured = capsys.readouterr()
        assert "WARNING" not in captured.out

    def test_warning_for_overlapping_files(self, copy_manager, capsys):
        """Test warning format when multiple files overlap."""
        # Set up first operation
        first_files = ["/tmp/app.log", "/tmp/error.log", "/tmp/debug.log"]
        copy_manager._copy_operations_files["copy1"] = set(first_files)

        # Second operation with multiple overlaps
        second_files = ["/tmp/app.log", "/tmp/error.log", "/tmp/unique.log"]
        copy_manager._check_for_duplicate_files("copy2", second_files)

        captured = capsys.readouterr()
        assert "WARNING" in captured.out
        assert "copying 2 file(s)" in captured.out
        assert "/tmp/app.log" in captured.out
        assert "/tmp/error.log" in captured.out
        assert "/tmp/unique.log" not in captured.out

    def test_no_warning_for_non_overlapping_files(self, copy_manager, capsys):
        """Test that no warning is issued when files don't overlap."""
        # Set up first operation
        copy_manager._copy_operations_files["copy1"] = {"/tmp/app.log", "/tmp/debug.log"}
        
        # Second operation with completely different files
        different_files = ["/tmp/other.log", "/tmp/new.log"]
        copy_manager._check_for_duplicate_files("copy2", different_files)
        
        captured = capsys.readouterr()
        assert "WARNING" not in captured.out

    def test_multiple_operations_overlapping(self, copy_manager, capsys):
        """Test warnings when multiple operations overlap with a new one."""
        # Set up multiple existing operations
        copy_manager._copy_operations_files["copy1"] = {"/tmp/app.log", "/tmp/debug.log"}
        copy_manager._copy_operations_files["copy2"] = {"/tmp/error.log", "/tmp/access.log"}
        
        # New operation overlapping with both
        third_files = ["/tmp/app.log", "/tmp/error.log", "/tmp/new.log"]
        copy_manager._check_for_duplicate_files("copy3", third_files)

        captured = capsys.readouterr()
        warning_lines = [line for line in captured.out.split('\n') if "WARNING" in line]
        assert len(warning_lines) == 2  # One warning for each overlap

    def test_file_tracking_updates_correctly(self, copy_manager):
        """Test that file tracking is updated after checking for duplicates."""
        files = ["/tmp/app.log", "/tmp/error.log"]
        
        # Initially no files tracked for this operation
        assert "copy1" not in copy_manager._copy_operations_files
        
        copy_manager._check_for_duplicate_files("copy1", files)
        
        # After checking, files should be tracked
        assert copy_manager._copy_operations_files["copy1"] == set(files)

    def test_empty_file_list_handling(self, copy_manager, capsys):
        """Test that empty file lists are handled gracefully."""
        copy_manager._copy_operations_files["copy1"] = {"/tmp/app.log"}
        
        # Check with empty file list
        copy_manager._check_for_duplicate_files("copy2", [])
        
        captured = capsys.readouterr()
        assert "WARNING" not in captured.out
        assert "copy2" not in copy_manager._copy_operations_files


class TestSignalHandling:
    """Test signal handling functionality."""
    
    def test_signal_handler_setup(self, mock_signal):
        """Test that signal handlers are set up correctly."""
        with patch.object(CopyManager, '_setup_signal_handlers') as mock_setup:
            manager = CopyManager()
            mock_setup.assert_called_once()

    @patch('src.main.logger._copy_manager.signal.signal')
    def test_signal_handlers_registered(self, mock_signal_signal):
        """Test that signal handlers are registered."""
        manager = CopyManager()
        
        # Should register handlers for SIGINT and SIGTERM
        assert mock_signal_signal.call_count >= 1


class TestCleanup:
    """Test cleanup functionality."""
    
    def test_cleanup_method_exists(self):
        """Test that cleanup method exists and is callable."""
        manager = CopyManager()
        assert hasattr(manager, 'cleanup')
        assert callable(manager.cleanup)

    def test_cleanup_stops_all_operations(self, mock_thread, mock_event):
        """Test that cleanup stops all copy operations."""
        manager = CopyManager(enabled=True)
        
        # Start some operations
        for i in range(2):
            params = {
                "copy_name": f"test_{i}",
                "path_patterns": [f"/tmp/test_{i}*.log"],
                "copy_destination": "hdfs://dest/",
                "copy_interval": 60
            }
            manager.start_copy(**params)
        
        # Add some file tracking to test cleanup
        manager._file_offsets["test_file.log"] = 100
        manager._file_sizes["test_file.log"] = 100
        
        # Cleanup should stop all operations
        manager.cleanup()
        
        assert len(manager._copy_threads) == 0
        assert len(manager._copy_operations_files) == 0
        assert len(manager._copy_operations_params) == 0
        assert len(manager._file_offsets) == 0
        assert len(manager._file_sizes) == 0

    def test_cleanup_can_be_called_multiple_times(self):
        """Test that cleanup can be called multiple times safely."""
        manager = CopyManager()
        
        # Should not raise any exceptions
        manager.cleanup()
        manager.cleanup()  # Second call should be safe


class TestFileDiscovery:
    """Test file discovery functionality."""
    
    @pytest.fixture
    def copy_manager(self):
        """Create a CopyManager instance for testing."""
        manager = CopyManager(enabled=True)
        yield manager
        manager.cleanup()

    def test_discover_files_to_copy_with_glob_patterns(self, copy_manager, sample_log_files):
        """Test file discovery with glob patterns."""
        with patch('glob.glob', return_value=sample_log_files):
            files = copy_manager._discover_files_to_copy(["/tmp/*.log"])
            assert len(files) == len(sample_log_files)
            assert all(f in files for f in sample_log_files)

    def test_discover_files_with_specific_paths(self, copy_manager, sample_log_files):
        """Test file discovery with specific file paths."""
        specific_files = sample_log_files[:2]  # Take first 2 files
        
        files = copy_manager._discover_files_to_copy(specific_files)
        assert len(files) == len(specific_files)
        assert all(f in files for f in specific_files)

    def test_discover_files_removes_duplicates(self, copy_manager):
        """Test that file discovery removes duplicate files."""
        duplicate_patterns = ["/tmp/file1.log", "/tmp/file1.log", "/tmp/file2.log"]
        
        with patch('glob.glob', side_effect=lambda x, recursive=False: [x] if x.endswith('.log') else []):
            with patch('os.path.isfile', return_value=True):
                files = copy_manager._discover_files_to_copy(duplicate_patterns)
                assert len(files) == 2  # Should remove duplicates
                assert "/tmp/file1.log" in files
                assert "/tmp/file2.log" in files


class TestIncrementalCopy:
    """Test incremental file copy logic using mocks."""
    
    @pytest.fixture
    def manager(self, mock_event, mock_thread):
        # Use enabled CopyManager with mocked threading
        mgr = CopyManager(enabled=True)
        yield mgr
        mgr.cleanup()

    def test_incremental_copy_only_new_content(self, manager, tmp_path):
        """Test the actual behavior of _incremental_copy_file method."""
        file_path = tmp_path / "test.log"
        dest_path = tmp_path / "dest.log"
        file_path.write_text("line1\nline2\n")
        
        # Mock the thread lock to avoid locking issues in tests
        mock_lock = MagicMock()
        mock_lock.__enter__ = MagicMock(return_value=mock_lock)
        mock_lock.__exit__ = MagicMock(return_value=None)
        
        with patch.object(manager, '_offset_lock', mock_lock), \
             patch.object(manager, '_file_offsets', {}, create=True), \
             patch.object(manager, '_file_sizes', {}, create=True), \
             patch('src.main.file_io.FileIOInterface.finfo') as mock_finfo, \
             patch('src.main.file_io.FileIOInterface.fexists') as mock_fexists, \
             patch('src.main.file_io.FileIOInterface.fopen') as mock_fopen:
            
            # Test 1: First call - destination doesn't exist
            mock_finfo.return_value = {'size': 6}
            mock_fexists.return_value = False
            
            bytes_copied = manager._incremental_copy_file(str(file_path), str(dest_path))
            # Current implementation returns 0 when destination doesn't exist
            assert bytes_copied == 0
            # Tracking should not be updated when destination doesn't exist
            assert str(file_path) not in manager._file_offsets
            assert str(file_path) not in manager._file_sizes
            
            # Test 2: Second call - destination exists, append new content
            # First, set up tracking manually since first call didn't update it
            manager._file_offsets[str(file_path)] = 6
            manager._file_sizes[str(file_path)] = 6
            
            mock_finfo.return_value = {'size': 12}
            mock_fexists.return_value = True
            
            # Properly mock the context managers
            mock_src = MagicMock()
            mock_src.read.return_value = b'line2\n'
            mock_src.seek.return_value = None
            mock_src.__enter__ = MagicMock(return_value=mock_src)
            mock_src.__exit__ = MagicMock(return_value=None)
            
            mock_dest = MagicMock()
            mock_dest.__enter__ = MagicMock(return_value=mock_dest)
            mock_dest.__exit__ = MagicMock(return_value=None)
            
            mock_fopen.side_effect = [mock_src, mock_dest]
            
            bytes_copied = manager._incremental_copy_file(str(file_path), str(dest_path))
            assert bytes_copied == 6  # Now it should copy new content
            assert manager._file_offsets[str(file_path)] == 12
            assert manager._file_sizes[str(file_path)] == 12
            mock_src.seek.assert_called_with(6)
            mock_dest.write.assert_called_with(b'line2\n')

    def test_incremental_copy_no_new_content(self, manager, tmp_path):
        """Test that no copy occurs when file size hasn't changed."""
        file_path = tmp_path / "test.log"
        dest_path = tmp_path / "dest.log"
        
        mock_lock = MagicMock()
        mock_lock.__enter__ = MagicMock(return_value=mock_lock)
        mock_lock.__exit__ = MagicMock(return_value=None)
        
        with patch.object(manager, '_offset_lock', mock_lock), \
             patch.object(manager, '_file_offsets', {str(file_path): 6}, create=True), \
             patch.object(manager, '_file_sizes', {str(file_path): 6}, create=True), \
             patch('src.main.file_io.FileIOInterface.finfo') as mock_finfo:
            
            # File size hasn't changed
            mock_finfo.return_value = {'size': 6}
            
            bytes_copied = manager._incremental_copy_file(str(file_path), str(dest_path))
            assert bytes_copied == 0  # No new content to copy

    def test_incremental_copy_file_truncated(self, manager, tmp_path, capsys):
        """Test handling of truncated/rotated files."""
        file_path = tmp_path / "test.log"
        dest_path = tmp_path / "dest.log"
        
        mock_lock = MagicMock()
        mock_lock.__enter__ = MagicMock(return_value=mock_lock)
        mock_lock.__exit__ = MagicMock(return_value=None)
        
        with patch.object(manager, '_offset_lock', mock_lock), \
             patch.object(manager, '_file_offsets', {str(file_path): 10}, create=True), \
             patch.object(manager, '_file_sizes', {str(file_path): 10}, create=True), \
             patch('src.main.file_io.FileIOInterface.finfo') as mock_finfo, \
             patch('src.main.file_io.FileIOInterface.fexists') as mock_fexists, \
             patch('src.main.file_io.FileIOInterface.fopen') as mock_fopen:
            
            # File was truncated (smaller than last known size)
            mock_finfo.return_value = {'size': 5}
            mock_fexists.return_value = True
            
            # Properly mock the context managers
            mock_src = MagicMock()
            mock_src.read.return_value = b'new\n'
            mock_src.seek.return_value = None
            mock_src.__enter__ = MagicMock(return_value=mock_src)
            mock_src.__exit__ = MagicMock(return_value=None)
            
            mock_dest = MagicMock()
            mock_dest.__enter__ = MagicMock(return_value=mock_dest)
            mock_dest.__exit__ = MagicMock(return_value=None)
            
            mock_fopen.side_effect = [mock_src, mock_dest]
            
            bytes_copied = manager._incremental_copy_file(str(file_path), str(dest_path))
            assert bytes_copied == 4  # Should copy all content from offset 0 (length of b'new\n')
            
            captured = capsys.readouterr()
            assert "rotated/truncated" in captured.out

    def test_incremental_copy_file_not_found(self, manager, tmp_path, capsys):
        """Test handling when file info cannot be retrieved."""
        file_path = tmp_path / "nonexistent.log"
        dest_path = tmp_path / "dest.log"
        
        mock_lock = MagicMock()
        mock_lock.__enter__ = MagicMock(return_value=mock_lock)
        mock_lock.__exit__ = MagicMock(return_value=None)
        
        with patch.object(manager, '_offset_lock', mock_lock), \
             patch.object(manager, '_file_offsets', {str(file_path): 5}, create=True), \
             patch.object(manager, '_file_sizes', {str(file_path): 5}, create=True), \
             patch('src.main.file_io.FileIOInterface.finfo') as mock_finfo, \
             patch('src.main.file_io.FileIOInterface.fexists') as mock_fexists:
            
            # Destination exists but file info cannot be retrieved
            mock_fexists.return_value = True
            mock_finfo.return_value = None
            
            bytes_copied = manager._incremental_copy_file(str(file_path), str(dest_path))
            assert bytes_copied == 0
            # Tracking should be reset
            assert str(file_path) not in manager._file_offsets
            assert str(file_path) not in manager._file_sizes
            
            captured = capsys.readouterr()
            assert "Could not get file info" in captured.out
