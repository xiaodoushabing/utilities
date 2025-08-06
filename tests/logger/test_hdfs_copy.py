"""
HDFS Copy Functionality Tests
"""

import pytest
import threading
import time
from pathlib import Path
from unittest.mock import patch

pytestmark = pytest.mark.unit

class TestHDFSCopyValidation:
    """Test HDFS copy parameter validation."""
    
    @pytest.mark.parametrize("invalid_params,expected_error", [
        ({"copy_name": ""}, "copy_name cannot be empty"),
        ({"path_patterns": []}, "path_patterns cannot be empty"), 
        ({"hdfs_destination": ""}, "hdfs_destination cannot be empty"),
        ({"copy_interval": 0}, "copy_interval must be positive"),
        ({"copy_interval": -1}, "copy_interval must be positive"),
        ({"max_retries": -1}, "max_retries cannot be negative"),
        ({"retry_delay": -1}, "retry_delay cannot be negative"),
        ({"preserve_structure": True, "root_dir": None}, "'root_dir' must be specified when 'preserve_structure' is True"),
    ])
    def test_parameter_validation(self, log_manager, invalid_params, expected_error):
        """Test that invalid parameters raise appropriate errors."""
        defaults = {
            "copy_name": "test",
            "path_patterns": ["/tmp/*_log.txt"],
            "hdfs_destination": "hdfs://dest/",
            "copy_interval": 60,
            "preserve_structure": False
        }
        params = {**defaults, **invalid_params}
        
        with pytest.raises(ValueError, match=expected_error):
            log_manager.start_hdfs_copy(**params)
    
    def test_duplicate_copy_name_rejected(self, log_manager):
        """Test that duplicate copy names are rejected."""
        copy_name = "duplicate_test"
        log_manager.start_hdfs_copy(copy_name, ["/tmp/*.log"], "hdfs://dest/", preserve_structure=False)
        
        with pytest.raises(ValueError, match="already exists"):
            log_manager.start_hdfs_copy(copy_name, ["/tmp/*.log"], "hdfs://dest/", preserve_structure=False)
        
        log_manager.stop_hdfs_copy(copy_name)


class TestHDFSCopyLifecycle:
    """Test HDFS copy operation lifecycle."""
    
    def test_start_creates_thread_and_tracking(self, log_manager):
        """Test that starting HDFS copy creates proper thread and tracking."""
        copy_name = "lifecycle_test"
        log_manager.start_hdfs_copy(copy_name, ["/tmp/*.log"], "hdfs://dest/", preserve_structure=False)
        
        # Verify thread creation
        assert copy_name in log_manager._hdfs_copy_threads
        assert copy_name in log_manager._stop_events
        
        thread = log_manager._hdfs_copy_threads[copy_name]
        assert thread.name == f"HDFSCopy-{copy_name}"
        assert thread.daemon is True
        assert thread.is_alive()
        
        log_manager.stop_hdfs_copy(copy_name)
    
    def test_stop_cleans_up_properly(self, log_manager):
        """Test that stopping HDFS copy removes all tracking."""
        copy_name = "stop_test"
        log_manager.start_hdfs_copy(copy_name, ["/tmp/*.log"], "hdfs://dest/", copy_interval=0.1, preserve_structure=False)
        
        result = log_manager.stop_hdfs_copy(copy_name)
        
        assert result is True
        assert copy_name not in log_manager._hdfs_copy_threads
        assert copy_name not in log_manager._stop_events
    
    def test_stop_nonexistent_raises_error(self, log_manager):
        """Test that stopping non-existent copy raises error."""
        with pytest.raises(ValueError, match="does not exist"):
            log_manager.stop_hdfs_copy("nonexistent")
    
    def test_list_operations_shows_active_copies(self, log_manager):
        """Test listing shows active copy operations."""
        assert log_manager.list_hdfs_copy_operations() == []
        
        copy_name = "list_test"
        log_manager.start_hdfs_copy(copy_name, ["/tmp/*.log"], "hdfs://dest/", preserve_structure=False)
        
        operations = log_manager.list_hdfs_copy_operations()
        assert len(operations) == 1
        assert operations[0]["name"] == copy_name
        assert operations[0]["daemon"] is True
        
        log_manager.stop_hdfs_copy(copy_name)


class TestFileDiscovery:
    """Test file discovery functionality."""
    
    @patch('utilities.logger.glob.glob')
    @patch('utilities.logger.os.path.isfile')
    def test_discover_files_mixed_patterns(self, mock_isfile, mock_glob, log_manager):
        """Test file discovery with mixed direct paths and glob patterns."""
        # Setup mocks
        mock_isfile.side_effect = lambda path: path in ["/direct/file.log", "/tmp/app.log", "/tmp/error.log"]
        mock_glob.return_value = ["/tmp/app.log", "/tmp/error.log"]
        
        patterns = ["/direct/file.log", "/tmp/*.log", "/missing/file.log"]
        result = log_manager._discover_files_to_copy(patterns)
        
        expected = {"/direct/file.log", "/tmp/app.log", "/tmp/error.log"}
        assert set(result) == expected
    
    def test_discover_files_with_real_filesystem(self, log_manager, temp_dir):
        """Test file discovery with actual files."""
        # Create test files
        log_file = Path(temp_dir) / "test.log"
        txt_file = Path(temp_dir) / "readme.txt"
        log_file.write_text("log content")
        txt_file.write_text("txt content")
        
        # Test glob pattern
        patterns = [f"{temp_dir}/*.log"]
        result = log_manager._discover_files_to_copy(patterns)
        assert str(log_file) in result
        assert str(txt_file) not in result


class TestFileCopying:
    """Test file copying operations."""
    
    @patch('utilities.logger.FileIOInterface.fcopy')
    @patch('utilities.logger.FileIOInterface.fmakedirs')
    def test_copy_files_basic_operation(self, mock_fmakedirs, mock_fcopy, log_manager):
        """Test basic file copying without structure preservation."""
        with patch('utilities.logger.os.path.basename', return_value="file.log"), \
             patch('utilities.logger.os.path.join', side_effect=lambda *args: "/".join(args)), \
             patch('utilities.logger.os.path.dirname', return_value="hdfs://dest"):
            
            log_manager._copy_files_to_hdfs(
                local_files=["/tmp/file.log"],
                hdfs_destination="hdfs://dest/",
                create_dest_dirs=True,
                preserve_structure=False,
                root_dir=None,
                max_retries=1,
                retry_delay=0
            )
        
        mock_fmakedirs.assert_called_once()
        mock_fcopy.assert_called_once_with(
            read_path="/tmp/file.log",
            dest_path="hdfs://dest//file.log"
        )
    
    @patch('utilities.logger.FileIOInterface.fcopy')
    @patch('utilities.logger.time.sleep')
    def test_copy_files_retry_logic(self, mock_sleep, mock_fcopy, log_manager):
        """Test retry logic when file copy fails."""
        # Fail once, then succeed
        mock_fcopy.side_effect = [Exception("Network error"), None]
        
        with patch('utilities.logger.os.path.basename', return_value="file.log"), \
             patch('utilities.logger.os.path.join', side_effect=lambda *args: "/".join(args)), \
             patch('utilities.logger.os.path.dirname', return_value="hdfs://dest"), \
             patch('utilities.logger.FileIOInterface.fmakedirs'):
            
            log_manager._copy_files_to_hdfs(
                local_files=["/tmp/file.log"],
                hdfs_destination="hdfs://dest/",
                create_dest_dirs=False,
                preserve_structure=False,
                root_dir=None,
                max_retries=2,
                retry_delay=0.1
            )
        
        assert mock_fcopy.call_count == 2
        mock_sleep.assert_called_once_with(0.1)


class TestWorkerThread:
    """Test HDFS copy worker thread behavior."""
    
    @patch('utilities.logger.LogManager._copy_files_to_hdfs')
    @patch('utilities.logger.LogManager._discover_files_to_copy')
    def test_worker_processes_files_when_found(self, mock_discover, mock_copy, log_manager):
        """Test worker processes files when they are discovered."""
        mock_discover.return_value = ["/tmp/file1.log", "/tmp/file2.log"]
        stop_event = threading.Event()
        
        def run_and_stop():
            # Let worker run briefly then stop it
            time.sleep(0.05)
            stop_event.set()
        
        stop_thread = threading.Thread(target=run_and_stop)
        stop_thread.start()
        
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
            stop_event=stop_event
        )
        
        stop_thread.join()
        mock_discover.assert_called()
        mock_copy.assert_called()


class TestMultipleOperations:
    """Test managing multiple HDFS copy operations."""
    
    def test_multiple_operations_independence(self, log_manager):
        """Test that multiple copy operations work independently."""
        copy_names = ["copy1", "copy2", "copy3"]
        
        # Start multiple operations
        for name in copy_names:
            log_manager.start_hdfs_copy(name, [f"/tmp/{name}/*.log"], f"hdfs://dest/{name}/", preserve_structure=False)
        
        # Verify all are tracked
        operations = log_manager.list_hdfs_copy_operations()
        assert len(operations) == 3
        running_names = [op["name"] for op in operations]
        assert set(running_names) == set(copy_names)
        
        # Stop all and verify cleanup
        failed = log_manager.stop_all_hdfs_copy()
        assert failed == []
        assert len(log_manager.list_hdfs_copy_operations()) == 0
    
    def test_cleanup_stops_all_operations(self, log_manager):
        """Test that LogManager cleanup stops all HDFS operations."""
        # Start operations
        log_manager.start_hdfs_copy("cleanup1", ["/tmp/*.log"], "hdfs://dest1/", preserve_structure=False)
        log_manager.start_hdfs_copy("cleanup2", ["/var/*.log"], "hdfs://dest2/", preserve_structure=False)
        
        assert len(log_manager._hdfs_copy_threads) == 2
        
        # Cleanup should stop everything
        log_manager._cleanup()
        
        assert len(log_manager._hdfs_copy_threads) == 0
        assert len(log_manager._stop_events) == 0
