"""
Copy Functionality Tests
"""

import pytest
import threading
import time
from pathlib import Path
from unittest.mock import patch, MagicMock, call

pytestmark = pytest.mark.unit

class TestCopyInstantiation:
        
    def test_initialization_sets_shutdown_false(self, mock_logger):
        """Test that LogManager initializes with shutdown flag as False."""
        log_manager = LogManager()
        assert log_manager._shutdown_in_progress is False
class TestCopyValidation:
    """Test copy parameter validation."""
    @pytest.mark.parametrize("invalid_params", [
        {"copy_name": ""}, 
        {"path_patterns": []}, 
        {"copy_destination": ""},
        {"copy_interval": 0},
        {"copy_interval": -1},
        {"max_retries": -1},
        {"retry_delay": -1},
        {"preserve_structure": True, "root_dir": None},
    ])
    def test_invalid_parameters_raise_error(self, log_manager, copy_defaults, invalid_params):
        # invalid_params will override defaults
        params = {**copy_defaults, **invalid_params}
        
        with pytest.raises(ValueError):
            log_manager.start_copy(**params)
    
    def test_duplicate_copy_name_rejected(self, mock_event, log_manager, copy_defaults, mock_thread):
        # First call should succeed and create tracking entries
        log_manager.start_copy(**copy_defaults)
        
        # Second call with same name should fail
        with pytest.raises(ValueError, match="already exists"):
            log_manager.start_copy(**copy_defaults)
    
    def test_cannot_start_copy_during_shutdown(self, log_manager, copy_defaults):
        """Test that copy operations cannot be started during shutdown."""
        # Simulate shutdown in progress
        log_manager._shutdown_in_progress = True
        
        with pytest.raises(ValueError, match="LogManager is shutting down"):
            log_manager.start_copy(**copy_defaults)


class TestCopyLifecycle:
    """Test copy operation lifecycle."""
    def test_start_creates_thread_and_tracking(self, mock_event, log_manager, copy_defaults, mock_thread):
        copy_name = copy_defaults["copy_name"]
        expected_thread_name = f"Copy-{copy_name}"
        log_manager.start_copy(**copy_defaults)
        mock_event.assert_called_once()
        mock_thread.assert_called_once()
        call_args = mock_thread.call_args[1]

        assert call_args['daemon'] is False
        assert call_args['name'] == expected_thread_name
        assert call_args['target'] == log_manager._copy_worker
        
        assert copy_name in log_manager._copy_threads
        assert copy_name in log_manager._stop_events
        assert log_manager._copy_threads[copy_name] == mock_thread.return_value
        assert log_manager._stop_events[copy_name] == mock_event.return_value

        mock_thread.return_value.start.assert_called_once()
    def test_stop_cleans_up_properly(self, mock_event, log_manager, copy_defaults, mock_thread):
        copy_name = copy_defaults["copy_name"]
        log_manager.start_copy(**copy_defaults)
        result = log_manager.stop_copy(copy_name)
        assert result is True
        mock_thread.return_value.join.assert_called_once()
        mock_event.return_value.set.assert_called_once()
        assert copy_name not in log_manager._copy_threads
        assert copy_name not in log_manager._stop_events
    
    def test_stop_nonexistent_raises_error(self, log_manager):
        """Test that stopping non-existent copy raises error."""
        with pytest.raises(ValueError, match="does not exist"):
            log_manager.stop_copy("nonexistent")
    def test_list_operations_shows_active_copies(self, mock_event, log_manager, copy_defaults, mock_thread):
        assert log_manager.list_copy_operations() == []
        copy_name = copy_defaults["copy_name"]
        log_manager.start_copy(**copy_defaults)
        operations = log_manager.list_copy_operations()
        assert len(operations) == 1
        assert operations[0]["name"] == copy_name
        assert operations[0]["thread_name"] == f"Copy-{copy_name}"
        assert operations[0]["daemon"] is False
        assert operations[0]["is_alive"] is True

    def test_stop_all_operations(self, mock_event, log_manager, copy_defaults, mock_thread):
        copy_name = copy_defaults["copy_name"]
        log_manager.start_copy(**copy_defaults)

        failed = log_manager.stop_all_copy()

        assert failed == []
        assert len(log_manager.list_copy_operations()) == 0
        assert copy_name not in log_manager._copy_threads
        assert copy_name not in log_manager._stop_events


class TestCopyOperationParameterStorage:
    """Test parameter storage and retrieval for copy operations."""

    def test_start_copy_stores_parameters(self, mock_thread, mock_event, log_manager, copy_defaults):
        """Test that start_copy stores operation parameters correctly."""
        copy_name = copy_defaults["copy_name"]

        log_manager.start_copy(**copy_defaults)

        # Verify parameters are stored
        assert copy_name in log_manager._copy_operations_params
        stored_params = log_manager._copy_operations_params[copy_name]
        
        # Check all expected parameters are stored
        expected_params = {
            'path_patterns': copy_defaults['path_patterns'],
            'copy_destination': copy_defaults['copy_destination'],
            'create_dest_dirs': copy_defaults.get('create_dest_dirs', True),
            'preserve_structure': copy_defaults.get('preserve_structure', False),
            'root_dir': copy_defaults.get('root_dir', None),
            'max_retries': copy_defaults.get('max_retries', 3),
            'retry_delay': copy_defaults.get('retry_delay', 5)
        }
        
        assert stored_params == expected_params

    def test_stop_copy_removes_parameters(self, mock_thread, mock_event, log_manager, copy_defaults):
        """Test that stop_copy removes stored parameters."""
        copy_name = copy_defaults["copy_name"]

        # Start and verify parameters are stored
        log_manager.start_copy(**copy_defaults)
        assert copy_name in log_manager._copy_operations_params
        
        # Stop and verify parameters are removed
        log_manager.stop_copy(copy_name)
        assert copy_name not in log_manager._copy_operations_params

    def test_cleanup_clears_all_parameters(self, mock_thread, mock_event, log_manager, copy_defaults):
        """Test that cleanup clears all stored parameters."""
        # Start multiple operations
        for i in range(3):
            params = {**copy_defaults, "copy_name": f"copy{i}"}
            log_manager.start_copy(**params)

        # Verify parameters are stored
        assert len(log_manager._copy_operations_params) == 3
        
        # Mock trigger to prevent hanging during cleanup
        with patch.object(log_manager, 'trigger_copy_now'):
            log_manager._cleanup()
        
        # Verify all parameters are cleared
        assert len(log_manager._copy_operations_params) == 0
    
    def test_parameter_storage_with_custom_values(self, mock_thread, mock_event, log_manager):
        """Test parameter storage with custom non-default values."""
        custom_params = {
            "copy_name": "custom_test",
            "path_patterns": ["/custom/*.log", "/other/*.txt"],
            "copy_destination": "hdfs://custom/dest/",
            "root_dir": "/custom/root",
            "copy_interval": 120,
            "create_dest_dirs": False,
            "preserve_structure": True,
            "max_retries": 5,
            "retry_delay": 10
        }

        log_manager.start_copy(**custom_params)

        stored_params = log_manager._copy_operations_params["custom_test"]
        
        # Verify custom values are stored correctly (excluding copy_interval which isn't stored)
        expected_stored = {
            'path_patterns': ["/custom/*.log", "/other/*.txt"],
            'copy_destination': "hdfs://custom/dest/",
            'create_dest_dirs': False,
            'preserve_structure': True,
            'root_dir': "/custom/root",
            'max_retries': 5,
            'retry_delay': 10
        }
        
        assert stored_params == expected_stored


class TestTriggerCopyNow:
    """Test manual trigger functionality for copy operations."""

    def test_trigger_single_operation(self, mock_thread, mock_event, log_manager, copy_defaults):
        """Test triggering a specific copy operation."""
        copy_name = copy_defaults["copy_name"]
        
        # Start operation
        log_manager.start_copy(**copy_defaults)
        
        # Mock the _perform_copy_operation method to verify it's called
        with patch.object(log_manager, '_perform_copy_operation') as mock_perform:
            log_manager.trigger_copy_now(copy_name)

            # Verify _perform_copy_operation was called with correct parameters
            mock_perform.assert_called_once_with(
                copy_name,
                copy_defaults['path_patterns'],
                copy_defaults['copy_destination'],
                copy_defaults.get('create_dest_dirs', True),
                copy_defaults.get('preserve_structure', False),
                copy_defaults.get('root_dir', None),
                copy_defaults.get('max_retries', 3),
                copy_defaults.get('retry_delay', 5)
            )

    def test_trigger_all_operations(self, mock_thread, mock_event, log_manager, copy_defaults):
        """Test triggering all active copy operations."""
        # Start multiple operations
        operation_names = ["op1", "op2", "op3"]
        for name in operation_names:
            params = {**copy_defaults, "copy_name": name}
            log_manager.start_copy(**params)
        
        # Mock the _perform_copy_operation method
        with patch.object(log_manager, '_perform_copy_operation') as mock_perform:
            log_manager.trigger_copy_now()  # No specific copy_name = trigger all

            # Verify _perform_copy_operation was called for each operation
            assert mock_perform.call_count == 3
            
            # Verify each operation was called with its stored parameters
            called_names = [call[0][0] for call in mock_perform.call_args_list]
            assert set(called_names) == set(operation_names)
    
    def test_trigger_nonexistent_operation_raises_error(self, mock_thread, mock_event, log_manager, copy_defaults):
        """Test that triggering a non-existent operation raises ValueError."""
        # Start one operation so that _copy_threads is not empty
        log_manager.start_copy(**copy_defaults)

        # Now trying to trigger a nonexistent operation should raise ValueError
        with pytest.raises(ValueError, match="does not exist"):
            log_manager.trigger_copy_now("nonexistent_copy")

    def test_trigger_with_no_active_operations(self, log_manager, capsys):
        """Test triggering when no operations are active."""
        log_manager.trigger_copy_now()

        captured = capsys.readouterr()
        assert "No active copy operations to trigger" in captured.out

    def test_trigger_handles_operation_exceptions(self, mock_thread, mock_event, log_manager, copy_defaults, capsys):
        """Test that trigger handles exceptions in individual operations gracefully."""
        copy_name = copy_defaults["copy_name"]

        # Start operation
        log_manager.start_copy(**copy_defaults)

        # Mock _perform_copy_operation to raise an exception
        with patch.object(log_manager, '_perform_copy_operation', side_effect=Exception("Copy error")):
            log_manager.trigger_copy_now(copy_name)

            # Verify error is logged but doesn't crash
            captured = capsys.readouterr()
            assert "Exception occured during manually-triggered copy operation" in captured.out
            assert copy_name in captured.out
            assert "Copy error" in captured.out
    
    def test_trigger_uses_stored_parameters_correctly(self, mock_thread, mock_event, log_manager):
        """Test that trigger uses the exact parameters that were stored during start."""
        custom_params = {
            "copy_name": "param_test",
            "path_patterns": ["/test/*.log"],
            "copy_destination": "hdfs://test/dest/",
            "root_dir": "/test/root",
            "create_dest_dirs": False,
            "preserve_structure": True,
            "max_retries": 7,
            "retry_delay": 15
        }

        log_manager.start_copy(**custom_params)

        with patch.object(log_manager, '_perform_copy_operation') as mock_perform:
            log_manager.trigger_copy_now("param_test")

            # Verify the exact stored parameters were used
            mock_perform.assert_called_once_with(
                "param_test",
                ["/test/*.log"],
                "hdfs://test/dest/",
                False,  # create_dest_dirs
                True,   # preserve_structure
                "/test/root",  # root_dir
                7,      # max_retries
                15      # retry_delay
            )


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
            
            log_manager._copy_files_to_dest(
                local_files=["/root_dir/app/logs/file.log"],
                copy_destination="hdfs://dest",
                create_dest_dirs=True,
                preserve_structure=True,
                root_dir="/root_dir/",
                max_retries=1,
                retry_delay=0
            )
        
        mock_fmakedirs.assert_called_once_with("hdfs://dest/app/logs", exist_ok=True)
        mock_fcopy.assert_called_once_with(
            read_path="/root_dir/app/logs/file.log",
            dest_path="hdfs://dest/app/logs/file.log"
        )
    
    @patch('utilities.logger.FileIOInterface.fcopy')
    @patch('utilities.logger.FileIOInterface.fmakedirs')
    def test_copy_files_preserve_structure_false(self, mock_fmakedirs, mock_fcopy, log_manager):
        with patch('utilities.logger.os.path.basename', return_value="file.log"), \
             patch('utilities.logger.os.path.join', side_effect=lambda *args: "/".join(args)), \
             patch('utilities.logger.os.path.dirname', return_value="hdfs://dest"):
            
            log_manager._copy_files_to_dest(
                local_files=["/complex/nested/path/file.log"],
                copy_destination="hdfs://dest",
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
            dest_path="hdfs://dest/file.log"
        )
    
    @patch('utilities.logger.FileIOInterface.fcopy')
    @patch('utilities.logger.FileIOInterface.fmakedirs')
    def test_copy_files_no_directory_creation(self, mock_fmakedirs, mock_fcopy, log_manager):
        with patch('utilities.logger.os.path.basename', return_value="file.log"), \
             patch('utilities.logger.os.path.join', side_effect=lambda *args: "/".join(args)):

            log_manager._copy_files_to_dest(
                local_files=["/tmp/file.log"],
                copy_destination="hdfs://dest",
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
            dest_path="hdfs://dest/file.log"
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

            log_manager._copy_files_to_dest(
                local_files=["/tmp/file.log"],
                copy_destination="hdfs://dest",
                create_dest_dirs=False,
                preserve_structure=False,
                root_dir=None,
                max_retries=3,
                retry_delay=0.1
            )
        
        assert mock_fcopy.call_count == 2
        assert mock_sleep.call_count == 1 
        mock_sleep.assert_called_once_with(0.1)
        
        mock_print.assert_any_call("Copy completed: 1 successful, 0 failed")
    
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

            log_manager._copy_files_to_dest(
                local_files=["/tmp/file.log"],
                copy_destination="hdfs://dest",
                create_dest_dirs=False,
                preserve_structure=False,
                root_dir=None,
                max_retries=2,  # Only 2 retries,
                retry_delay=0.1
            )
        
        assert mock_fcopy.call_count == 3  # 1 initial + 2 retries
        assert mock_sleep.call_count == 2   # 2 sleeps between attempts

        mock_print.assert_any_call("Copy completed: 0 successful, 1 failed")


class TestWorkerThread:
    """Test copy worker thread behavior."""

    @patch('utilities.logger.LogManager._copy_files_to_dest')
    def test_worker_processes_files_when_found(self, mock_copy, mock_file_discovery, mock_event, log_manager):
        """Test worker processes files when they are discovered."""
        mock_file_discovery.return_value = ["/tmp/file1.log", "/tmp/file2.log"]
        
        mock_stop_event = mock_event.return_value
        
        # First call returns False (continue), second call returns True (stop)
        mock_stop_event.is_set.side_effect = [False, True]
        
        mock_stop_event.wait.return_value = False
        
        log_manager._copy_worker(
            copy_name="test_worker",
            path_patterns=["/tmp/*.log"],
            copy_destination="hdfs://dest",
            copy_interval=0.1,
            create_dest_dirs=True,
            preserve_structure=False,
            root_dir=None,
            max_retries=1,
            retry_delay=0,
            stop_event=mock_stop_event
        )
        
        mock_file_discovery.assert_called_with(["/tmp/*.log"])
        mock_copy.assert_called_with(
            ["/tmp/file1.log", "/tmp/file2.log"],
            "hdfs://dest",
            True,  # create_dest_dirs
            False,  # preserve_structure
            None,  # root_dir
            1,  # max_retries
            0   # retry_delay
        )
        
        # Verify the stop event was checked
        assert mock_stop_event.is_set.call_count == 2
        mock_stop_event.wait.assert_called_once_with(timeout=0.1)

    @patch('utilities.logger.LogManager._copy_files_to_dest')
    def test_worker_stops_when_stop_event_is_set(self, mock_copy, mock_file_discovery, mock_event, log_manager):
        mock_file_discovery.return_value = ["/tmp/file1.log", "/tmp/file2.log"]
        
        mock_stop_event = mock_event.return_value
        
       # is_set() returns False for both iterations, wait() times out first time then detects stop event
        mock_stop_event.is_set.side_effect = [False, False, False]

        # First wait() times out (False), second wait() detects stop event (True) and exits loop
        mock_stop_event.wait.side_effect = [False, True]
        
        log_manager._copy_worker(
            copy_name="test_worker_stop",
            path_patterns=["/tmp/*.log"],
            copy_destination="hdfs://dest",
            copy_interval=0.1,
            create_dest_dirs=True,
            preserve_structure=False,
            root_dir=None,
            max_retries=1,
            retry_delay=0,
            stop_event=mock_stop_event
        )
        
        # Verify that files were discovered and copied twice 
        assert mock_file_discovery.call_count == 2
        assert mock_copy.call_count == 2
        
        # Verify the stop event was checked only once (loop exited via wait() returning True)
        assert mock_stop_event.is_set.call_count == 2
        assert mock_stop_event.wait.call_count == 2


class TestCleanup:
    """Test LogManager cleanup functionality."""
    
    def test_cleanup_stops_all_copy_operations_and_clears_mappings(self, mock_event, mock_logger, log_manager, copy_defaults, mock_thread):
        copy_name1 = "cleanup_test1"
        copy_name2 = "cleanup_test2"

        params1 = {**copy_defaults, "copy_name": copy_name1}
        log_manager.start_copy(**params1)

        params2 = {**copy_defaults, "copy_name": copy_name2}
        log_manager.start_copy(**params2)

        # Verify parameters are stored before cleanup
        assert len(log_manager._copy_operations_params) == 2
        assert copy_name1 in log_manager._copy_operations_params
        assert copy_name2 in log_manager._copy_operations_params
        
        mock_logger.remove.reset_mock()
        # Mock trigger_copy_now to prevent hanging during cleanup
        with patch.object(log_manager, 'trigger_copy_now'):
            log_manager._cleanup()

        assert len(log_manager._copy_threads) == 0
        assert len(log_manager._stop_events) == 0
        assert len(log_manager._copy_operations_params) == 0  # Verify parameters are cleared
        mock_logger.remove.assert_called_once()

    @pytest.mark.parametrize("failed_ops", [
        ["failed_op1", "failed_op2"],
        []
    ])
    def test_cleanup_completes_with_or_without_copy_stop_failures(self, mock_logger, log_manager, failed_ops):
        mock_logger.remove.reset_mock()

        # mock stop_all_copy to return failed operations
        with patch.object(log_manager, 'stop_all_copy', return_value=failed_ops) as mock_stop_all:
            # Mock trigger_copy_now to prevent hanging during cleanup
            with patch.object(log_manager, 'trigger_copy_now'):
                log_manager._cleanup()
            
            # Verify cleanup still completes even with failed operations
            mock_stop_all.assert_called_once()
        
        # Verify cleanup completed successfully despite failed operations
        mock_logger.remove.assert_called_once()
        assert log_manager._shutdown_in_progress is True


    def test_cleanup_idempotency(self, mock_event, log_manager, copy_defaults, mock_thread):
        """Test that cleanup can be called multiple times safely."""
        # Start a copy operation
        log_manager.start_copy(**copy_defaults)
        # Verify operation is running
        assert len(log_manager._copy_threads) == 1
        assert log_manager._shutdown_in_progress is False
        
        # Call cleanup
        log_manager._cleanup()
        # Verify cleanup occurred
        assert log_manager._shutdown_in_progress is True
        assert len(log_manager._copy_threads) == 0

        # Call cleanup again, second call should be safe
        log_manager._cleanup()
        
        # State should remain consistent
        assert log_manager._shutdown_in_progress is True
        assert len(log_manager._copy_threads) == 0

    def test_cleanup_calls_trigger_before_stopping_operations(self, mock_thread, mock_event, log_manager, copy_defaults):
        """Test that cleanup calls trigger_copy_now before stopping operations."""
        # Start operations
        log_manager.start_copy(**copy_defaults)

        # Track the order of calls
        call_order = []
        
        def mock_trigger():
            call_order.append("trigger")
        
        def mock_stop_all(*args, **kwargs):
            call_order.append("stop_all")
            return []  # No failed operations

        with patch.object(log_manager, 'trigger_copy_now', side_effect=mock_trigger):
            with patch.object(log_manager, 'stop_all_copy', side_effect=mock_stop_all):
                log_manager._cleanup()
        
        # Verify trigger was called before stop_all
        assert call_order == ["trigger", "stop_all"]
    
    def test_cleanup_skips_trigger_when_no_operations(self, log_manager, capsys):
        """Test that cleanup skips trigger when no copy operations are active."""
        # No operations started
        assert len(log_manager._copy_threads) == 0

        with patch.object(log_manager, 'trigger_copy_now') as mock_trigger:
            log_manager._cleanup()
            
            # Trigger should not be called when no operations exist
            mock_trigger.assert_not_called()

    def test_cleanup_proceeds_despite_trigger_exception(self, mock_thread, mock_event, log_manager, copy_defaults, capsys):
        """Test that cleanup continues even if trigger_copy_now raises an exception."""
        log_manager.start_copy(**copy_defaults)

        # Mock trigger to raise exception
        with patch.object(log_manager, 'trigger_copy_now', side_effect=Exception("Trigger failed")):
            with patch.object(log_manager, 'stop_all_copy') as mock_stop:
                # Cleanup should not crash despite trigger exception
                log_manager._cleanup()
                
                # Verify stop_all was still called after the trigger exception
                mock_stop.assert_called_once()
                
                # Verify shutdown flag is still set
                assert log_manager._shutdown_in_progress is True
                
                # Verify warning message was printed
                captured = capsys.readouterr()
                assert "Warning: Final copy failed during cleanup: Trigger failed" in captured.out
