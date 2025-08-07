"""
HDFS Copy Functionality Tests
"""

import pytest
import threading
import time
from pathlib import Path
from unittest.mock import patch, MagicMock, call

pytestmark = pytest.mark.unit

class TestHStartDFSCopyValidation:
    """Test HDFS copy parameter validation."""
    
    @pytest.mark.parametrize("invalid_params", [
        {"copy_name": ""}, 
        {"path_patterns": []}, 
        {"hdfs_destination": ""},
        {"copy_interval": 0},
        {"copy_interval": -1},
        {"max_retries": -1},
        {"retry_delay": -1},
        {"preserve_structure": True, "root_dir": None},
    ])
    def test_invalid_parameters_raise_error(self, log_manager, hdfs_copy_defaults, invalid_params):
        # invalid_params will override defaults
        params = {**hdfs_copy_defaults, **invalid_params}
        
        with pytest.raises(ValueError):
            log_manager.start_hdfs_copy(**params)
    
    def test_duplicate_copy_name_rejected(self, mock_event, log_manager, hdfs_copy_defaults, mock_thread):
        # First call should succeed and create tracking entries
        log_manager.start_hdfs_copy(**hdfs_copy_defaults)
        
        # Second call with same name should fail
        with pytest.raises(ValueError, match="already exists"):
            log_manager.start_hdfs_copy(**hdfs_copy_defaults)


class TestHDFSCopyLifecycle:
    """Test HDFS copy operation lifecycle."""
    
    def test_start_creates_thread_and_tracking(self, mock_event, log_manager, hdfs_copy_defaults, mock_thread):
        copy_name = hdfs_copy_defaults["copy_name"]
        expected_thread_name = f"HDFSCopy-{copy_name}"
        
        log_manager.start_hdfs_copy(**hdfs_copy_defaults)

        mock_event.assert_called_once()
        mock_thread.assert_called_once()
        call_args = mock_thread.call_args[1]

        assert call_args['daemon'] is True
        assert call_args['name'] == expected_thread_name
        assert call_args['target'] == log_manager._hdfs_copy_worker
        
        assert copy_name in log_manager._hdfs_copy_threads
        assert copy_name in log_manager._stop_events
        assert log_manager._hdfs_copy_threads[copy_name] == mock_thread.return_value
        assert log_manager._stop_events[copy_name] == mock_event.return_value

        mock_thread.return_value.start.assert_called_once()
    
    def test_stop_cleans_up_properly(self, mock_event, log_manager, hdfs_copy_defaults, mock_thread):
        copy_name = hdfs_copy_defaults["copy_name"]
        log_manager.start_hdfs_copy(**hdfs_copy_defaults)
        
        result = log_manager.stop_hdfs_copy(copy_name)
        
        assert result is True
        mock_thread.return_value.join.assert_called_once()
        mock_event.return_value.set.assert_called_once()
        assert copy_name not in log_manager._hdfs_copy_threads
        assert copy_name not in log_manager._stop_events
    
    def test_stop_nonexistent_raises_error(self, log_manager):
        """Test that stopping non-existent copy raises error."""
        with pytest.raises(ValueError, match="does not exist"):
            log_manager.stop_hdfs_copy("nonexistent")
    
    def test_list_operations_shows_active_copies(self, log_manager, hdfs_copy_defaults):
        assert log_manager.list_hdfs_copy_operations() == []

        copy_name = hdfs_copy_defaults["copy_name"]
        log_manager.start_hdfs_copy(**hdfs_copy_defaults)
        
        operations = log_manager.list_hdfs_copy_operations()
        assert len(operations) == 1
        assert operations[0]["name"] == copy_name
        assert operations[0]["thread_name"] == f"HDFSCopy-{copy_name}"
        assert operations[0]["daemon"] is True
        assert operations[0]["is_alive"] is True

    def test_stop_all_operations(self, log_manager, hdfs_copy_defaults):
        copy_name = hdfs_copy_defaults["copy_name"]
        log_manager.start_hdfs_copy(**hdfs_copy_defaults)
        
        failed = log_manager.stop_all_hdfs_copy()
        
        assert failed == []
        assert len(log_manager.list_hdfs_copy_operations()) == 0
        assert copy_name not in log_manager._hdfs_copy_threads
        assert copy_name not in log_manager._stop_events

class TestFileDiscovery:
    """Test file discovery functionality."""
    
    @patch('utilities.logger.glob.glob')
    @patch('utilities.logger.os.path.isfile')
    def test_discover_files(self, mock_isfile, mock_glob, log_manager):
        mock_isfile.side_effect = lambda path: path in ["/direct/file.txt", "/tmp/app.log", "/tmp/error.log"]
        mock_glob.return_value = ["/tmp/app.log", "/tmp/error.log"]

        patterns = ["/direct/file.txt", "/tmp/*.log", "/missing/file.log", "/a_directory/"]
        result = log_manager._discover_files_to_copy(patterns)
        
        expected = {"/direct/file.txt", "/tmp/app.log", "/tmp/error.log"}
        assert set(result) == expected

class TestFileCopying:
    """Test file copying operations."""
    
    @patch('utilities.logger.FileIOInterface.fcopy')
    @patch('utilities.logger.FileIOInterface.fmakedirs')
    def test_copy_files_preserve_structure_true(self, mock_fmakedirs, mock_fcopy, log_manager):
        with patch('utilities.logger.os.path.relpath', return_value="app/logs/file.log"), \
             patch('utilities.logger.os.path.join', side_effect=lambda *args: "/".join(args)), \
             patch('utilities.logger.os.path.dirname', return_value="hdfs://dest/app/logs"):
            
            log_manager._copy_files_to_hdfs(
                local_files=["/root_dir/app/logs/file.log"],
                hdfs_destination="hdfs://dest/",
                create_dest_dirs=True,
                preserve_structure=True,
                root_dir="/root_dir/",
                max_retries=1,
                retry_delay=0
            )
        
        mock_fmakedirs.assert_called_once_with("hdfs://dest/app/logs", exist_ok=True)
        mock_fcopy.assert_called_once_with(
            read_path="/root_dir/app/logs/file.log",
            dest_path="hdfs://dest//app/logs/file.log" 
        )
    
    @patch('utilities.logger.FileIOInterface.fcopy')
    @patch('utilities.logger.FileIOInterface.fmakedirs')
    def test_copy_files_preserve_structure_false(self, mock_fmakedirs, mock_fcopy, log_manager):
        with patch('utilities.logger.os.path.basename', return_value="file.log"), \
             patch('utilities.logger.os.path.join', side_effect=lambda *args: "/".join(args)), \
             patch('utilities.logger.os.path.dirname', return_value="hdfs://dest"):
            
            log_manager._copy_files_to_hdfs(
                local_files=["/complex/nested/path/file.log"],
                hdfs_destination="hdfs://dest/",
                create_dest_dirs=True,
                preserve_structure=False,
                root_dir=None,
                max_retries=1,
                retry_delay=0
            )
        
        # Verify only filename is used, no nested structure
        mock_fmakedirs.assert_called_once_with("hdfs://dest", exist_ok=True)
        mock_fcopy.assert_called_once_with(
            read_path="/complex/nested/path/file.log",
            dest_path="hdfs://dest//file.log"
        )
    
    @patch('utilities.logger.FileIOInterface.fcopy')
    @patch('utilities.logger.FileIOInterface.fmakedirs')
    def test_copy_files_no_directory_creation(self, mock_fmakedirs, mock_fcopy, log_manager):
        with patch('utilities.logger.os.path.basename', return_value="file.log"), \
             patch('utilities.logger.os.path.join', side_effect=lambda *args: "/".join(args)):
            
            log_manager._copy_files_to_hdfs(
                local_files=["/tmp/file.log"],
                hdfs_destination="hdfs://dest/",
                create_dest_dirs=False,
                preserve_structure=False,
                root_dir=None,
                max_retries=1,
                retry_delay=0
            )
        
        # Verify no directory creation occurs
        mock_fmakedirs.assert_not_called()
        mock_fcopy.assert_called_once_with(
            read_path="/tmp/file.log",
            dest_path="hdfs://dest//file.log"
        )
    
    @patch('utilities.logger.FileIOInterface.fcopy')
    @patch('utilities.logger.time.sleep')
    @patch('builtins.print')
    def test_copy_files_retry_logic(self, mock_print, mock_sleep, mock_fcopy, log_manager):
        mock_fcopy.side_effect = [
            Exception("Network error"), 
            None  # Success
        ]
        
        with patch('utilities.logger.os.path.basename', return_value="file.log"), \
             patch('utilities.logger.os.path.join', side_effect=lambda *args: "/".join(args)):

            log_manager._copy_files_to_hdfs(
                local_files=["/tmp/file.log"],
                hdfs_destination="hdfs://dest/",
                create_dest_dirs=False,
                preserve_structure=False,
                root_dir=None,
                max_retries=3,
                retry_delay=0.1
            )
        
        assert mock_fcopy.call_count == 2
        assert mock_sleep.call_count == 1 
        mock_sleep.assert_called_once_with(0.1)
        
        mock_print.assert_any_call("HDFS copy completed: 1 successful, 0 failed")
    
    @patch('utilities.logger.FileIOInterface.fcopy')
    @patch('utilities.logger.time.sleep')
    @patch('builtins.print')
    def test_copy_files_retry_exhausted(self, mock_print, mock_sleep, mock_fcopy, log_manager):
        # Always fail - no success
        mock_fcopy.side_effect = [
            Exception("Network error"), 
            Exception("Timeout error"),
            Exception("Final error")
        ]
        
        with patch('utilities.logger.os.path.basename', return_value="file.log"), \
             patch('utilities.logger.os.path.join', side_effect=lambda *args: "/".join(args)):

            log_manager._copy_files_to_hdfs(
                local_files=["/tmp/file.log"],
                hdfs_destination="hdfs://dest/",
                create_dest_dirs=False,
                preserve_structure=False,
                root_dir=None,
                max_retries=2,  # Only 2 retries,
                retry_delay=0.1
            )
        
        assert mock_fcopy.call_count == 3  # 1 initial + 2 retries
        assert mock_sleep.call_count == 2   # 2 sleeps between attempts
        
        mock_print.assert_any_call("HDFS copy completed: 0 successful, 1 failed")


class TestWorkerThread:
    """Test HDFS copy worker thread behavior."""
    
    @patch('utilities.logger.LogManager._copy_files_to_hdfs')
    @patch('utilities.logger.LogManager._discover_files_to_copy')
    def test_worker_processes_files_when_found(self, mock_discover, mock_copy, mock_event, log_manager):
        """Test worker processes files when they are discovered."""
        mock_discover.return_value = ["/tmp/file1.log", "/tmp/file2.log"]
        
        mock_stop_event = mock_event.return_value
        
        # First call returns False (continue), second call returns True (stop)
        mock_stop_event.is_set.side_effect = [False, True]
        
        mock_stop_event.wait.return_value = False
        
        log_manager._hdfs_copy_worker(
            copy_name="test_worker",
            path_patterns=["/tmp/*.log"],
            hdfs_destination="hdfs://dest/",
            copy_interval=0.1,
            create_dest_dirs=True,
            preserve_structure=False,
            root_dir=None,
            max_retries=1,
            retry_delay=0,
            stop_event=mock_stop_event
        )
        
        mock_discover.assert_called_with(["/tmp/*.log"])
        mock_copy.assert_called_with(
            ["/tmp/file1.log", "/tmp/file2.log"],
            "hdfs://dest/",
            True,  # create_dest_dirs
            False,  # preserve_structure
            None,  # root_dir
            1,  # max_retries
            0   # retry_delay
        )
        
        # Verify the stop event was checked
        assert mock_stop_event.is_set.call_count == 2
        mock_stop_event.wait.assert_called_once_with(timeout=0.1)

    @patch('utilities.logger.LogManager._copy_files_to_hdfs')
    @patch('utilities.logger.LogManager._discover_files_to_copy')
    def test_worker_stops_when_stop_event_is_set(self, mock_discover, mock_copy, mock_event, log_manager):
        mock_discover.return_value = ["/tmp/file1.log", "/tmp/file2.log"]
        
        mock_stop_event = mock_event.return_value
        
        # First call returns False (continue), then wait() returns True so loop exits
        mock_stop_event.is_set.side_effect = [False, False, False]
        
        # Mock wait to return True (stop event was set during wait - someone called stop_hdfs_copy)
        mock_stop_event.wait.side_effect = [False, True]
        
        log_manager._hdfs_copy_worker(
            copy_name="test_worker_stop",
            path_patterns=["/tmp/*.log"],
            hdfs_destination="hdfs://dest/",
            copy_interval=0.1,
            create_dest_dirs=True,
            preserve_structure=False,
            root_dir=None,
            max_retries=1,
            retry_delay=0,
            stop_event=mock_stop_event
        )
        
        # Verify that files were discovered and copied twice 
        assert mock_discover.call_count == 2
        assert mock_copy.call_count == 2
        
        # Verify the stop event was checked only once (loop exited via wait() returning True)
        assert mock_stop_event.is_set.call_count == 2
        assert mock_stop_event.wait.call_count == 2


class TestCleanup:
    """Test LogManager cleanup functionality."""
    
    def test_cleanup_stops_all_hdfs_operations_and_clears_mappings(self, mock_logger, log_manager, hdfs_copy_defaults):
        copy_name1 = "cleanup_test1"
        copy_name2 = "cleanup_test2"
        
        params1 = {**hdfs_copy_defaults, "copy_name": copy_name1}
        log_manager.start_hdfs_copy(**params1)
        
        params2 = {**hdfs_copy_defaults, "copy_name": copy_name2}
        log_manager.start_hdfs_copy(**params2)
        
        mock_logger.remove.reset_mock()
        log_manager._cleanup()
        
        assert len(log_manager._hdfs_copy_threads) == 0
        assert len(log_manager._stop_events) == 0
        mock_logger.remove.assert_called_once()
    
    @patch('builtins.print')
    def test_cleanup_handles_failed_hdfs_stop_operations(self, mock_print, mock_logger, log_manager):
        mock_logger.remove.reset_mock()
        
        # mock stop_all_hdfs_copy to return failed operations
        with patch.object(log_manager, 'stop_all_hdfs_copy', return_value=["failed_op1", "failed_op2"]):
            log_manager._cleanup()
        
        mock_print.assert_called_with(
            "Warning: Some HDFS copy operations did not stop cleanly: ['failed_op1', 'failed_op2']\n"
            "Please check the threads manually."
        )
    
    def test_cleanup_with_no_hdfs_operations(self, mock_logger, log_manager):
        with patch.object(log_manager, 'stop_all_hdfs_copy', return_value=[]) as mock_stop_all:
            # Call cleanup
            log_manager._cleanup()
            
            # Verify stop_all_hdfs_copy was called once
            mock_stop_all.assert_called_once()
