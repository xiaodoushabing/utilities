import pytest
from unittest.mock import patch, MagicMock

pytestmark = pytest.mark.integration

class TestMultipleOperations:
    """Test managing multiple HDFS copy operations."""
    
    def test_multiple_operations_independence(self, mock_thread, mock_event, log_manager):
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
    
    def test_cleanup_stops_all_operations(self, mock_thread, mock_event, log_manager):
        """Test that LogManager cleanup stops all HDFS operations."""
        # Start operations
        log_manager.start_hdfs_copy("cleanup1", ["/tmp/*.log"], "hdfs://dest1/", preserve_structure=False)
        log_manager.start_hdfs_copy("cleanup2", ["/var/*.log"], "hdfs://dest2/", preserve_structure=False)
        
        assert len(log_manager._hdfs_copy_threads) == 2
        
        # Mock trigger_hdfs_copy_now to prevent hanging during cleanup
        with patch.object(log_manager, 'trigger_hdfs_copy_now'):
            # Cleanup should stop everything
            log_manager._cleanup()
        
        assert len(log_manager._hdfs_copy_threads) == 0
        assert len(log_manager._stop_events) == 0


@patch('utilities.logger.atexit.register')
class TestCleanupIntegration:
    """Test integration between signal handlers and cleanup functionality."""

    @patch('utilities.logger.LogManager.stop_all_hdfs_copy')
    @patch('utilities.logger.logger.remove')
    @patch('builtins.print')
    def test_cleanup_performs_all_operations(self, mock_print, mock_logger_remove, mock_stop_all, mock_atexit, log_manager):
        """Test that cleanup performs all required cleanup operations."""
        # Add some state to cleanup
        log_manager._handlers_map["test_handler"] = {"id": "123"}
        log_manager._loggers_map["test_logger"] = [{"handler": "test_handler", "level": "INFO"}]
        log_manager._copy_operations_files["test_op"] = {"/tmp/test.log"}
        log_manager._copy_operations_params["test_op"] = {"path_patterns": ["/tmp/*.log"]}
        
        # Add a mock thread to trigger the trigger_hdfs_copy_now call
        mock_thread = MagicMock()
        log_manager._hdfs_copy_threads["test_op"] = mock_thread
        
        # Mock trigger_hdfs_copy_now to prevent hanging during cleanup
        with patch.object(log_manager, 'trigger_hdfs_copy_now') as mock_trigger:
            # Call cleanup
            log_manager._cleanup(hdfs_timeout=30.0)
        
        # Verify all cleanup operations were performed
        mock_stop_all.assert_called_once_with(timeout=30.0, verbose=True)
        mock_logger_remove.assert_called_once()
        mock_trigger.assert_called_once()  # Should be called since we have threads
        
        # Verify all internal state was cleared
        assert len(log_manager._handlers_map) == 0
        assert len(log_manager._loggers_map) == 0
        assert len(log_manager._copy_operations_files) == 0
        assert len(log_manager._copy_operations_params) == 0
        assert log_manager._shutdown_in_progress is True
        
        # Verify proper logging occurred
        mock_print.assert_any_call("LogManager cleanup initiated...")
        mock_print.assert_any_call("LogManager cleanup completed.")


class TestConcurrentOperations:
    """Test concurrent HDFS operations and their interactions."""
    
    def test_stop_all_with_timeout_failures(self, mock_thread, mock_event, log_manager):
        """Test stop_all_hdfs_copy when some operations fail to stop within timeout."""
        # Start multiple operations
        for i in range(3):
            log_manager.start_hdfs_copy(f"op{i}", [f"/tmp/op{i}/*.log"], f"hdfs://dest{i}/", preserve_structure=False)
        
        # Mock stop_hdfs_copy to simulate timeout failures for some operations
        original_stop = log_manager.stop_hdfs_copy
        def mock_stop(copy_name, timeout):
            # Simulate timeout failure for op1
            if copy_name == "op1":
                return False
            return original_stop(copy_name, timeout)
        
        with patch.object(log_manager, 'stop_hdfs_copy', side_effect=mock_stop):
            failed_ops = log_manager.stop_all_hdfs_copy(timeout=0.1, verbose=True)
            
            # Should return failed operations
            assert "op1" in failed_ops
            assert len(failed_ops) == 1
    
    def test_worker_exception_during_cleanup(self, mock_thread, mock_event, log_manager):
        """Test cleanup behavior when worker threads encounter exceptions during operation."""
        # Start an operation
        log_manager.start_hdfs_copy("error_prone", ["/tmp/*.log"], "hdfs://dest/", preserve_structure=False)
        
        # Mock the worker to raise an exception during operation
        with patch.object(log_manager, '_discover_files_to_copy', side_effect=Exception("Worker error")):
            # Mock trigger_hdfs_copy_now to prevent hanging during cleanup
            with patch.object(log_manager, 'trigger_hdfs_copy_now'):
                # Cleanup should still work despite worker exceptions
                log_manager._cleanup()
            
            assert log_manager._shutdown_in_progress is True
            assert len(log_manager._hdfs_copy_threads) == 0

    def test_concurrent_operations_with_file_overlap(self, mock_thread, mock_event, log_manager):
        """Test that concurrent operations handle overlapping files correctly."""
        # This tests integration between duplicate file detection and multiple workers
        with patch('utilities.logger.LogManager._discover_files_to_copy') as mock_discover:
            # Both operations discover the same files
            mock_discover.return_value = ["/tmp/shared.log", "/tmp/app.log"]
            
            # Start two operations that will discover overlapping files
            log_manager.start_hdfs_copy("copy1", ["/tmp/*.log"], "hdfs://dest1/", preserve_structure=False)
            log_manager.start_hdfs_copy("copy2", ["/var/*.log"], "hdfs://dest2/", preserve_structure=False)
            
            # Verify both operations are tracked
            operations = log_manager.list_hdfs_copy_operations()
            assert len(operations) == 2
            
            # Mock trigger_hdfs_copy_now to prevent hanging during cleanup
            with patch.object(log_manager, 'trigger_hdfs_copy_now') as mock_trigger:
                # Cleanup should stop both operations
                log_manager._cleanup()
                assert len(log_manager._hdfs_copy_threads) == 0
                # Verify trigger was called during cleanup
                mock_trigger.assert_called_once()


class TestSignalIntegration:
    """Test integration between signal handling and HDFS operations."""
    
    @patch('utilities.logger.signal.signal')
    @patch('utilities.logger.os.kill')
    def test_signal_cleanup_with_active_hdfs_operations(self, mock_kill, mock_signal, mock_thread, mock_event, log_manager):
        """Test that signal-triggered cleanup properly handles active HDFS operations."""
        # Start HDFS operations
        log_manager.start_hdfs_copy("active1", ["/tmp/*.log"], "hdfs://dest1/", preserve_structure=False)
        log_manager.start_hdfs_copy("active2", ["/var/*.log"], "hdfs://dest2/", preserve_structure=False)
        
        # Setup signal handler
        log_manager._setup_signal_handlers()
        signal_handler = mock_signal.call_args_list[0][0][1]
        
        # Verify operations are running
        assert len(log_manager._hdfs_copy_threads) == 2
        
        # Simulate signal reception
        signal_handler(2, None)  # SIGINT
        
        # Verify all operations were stopped
        assert len(log_manager._hdfs_copy_threads) == 0
        assert log_manager._shutdown_in_progress is True
        
        # Verify signal was re-raised
        mock_kill.assert_called_once()
