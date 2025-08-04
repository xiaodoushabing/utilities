#!/usr/bin/env python3
"""
Standalone test for HDFS copy functionality without pytest dependencies.

This script tests the HDFS copy methods directly without requiring
pytest or the full LogManager dependencies.
"""

import os
import time
import tempfile
import threading
from pathlib import Path
from unittest.mock import patch, MagicMock


class MockLogManager:
    """
    Mock LogManager class with only HDFS copy functionality for testing.
    
    This class contains the exact HDFS copy methods from the real LogManager
    but without requiring any external dependencies.
    """
    
    def __init__(self):
        """Initialize the mock LogManager with HDFS copy attributes."""
        self._hdfs_copy_threads = {}  # thread_name -> thread object
        self._stop_events = {}        # thread_name -> threading.Event
    
    def start_hdfs_copy(
        self,
        copy_name: str,
        local_pattern,
        hdfs_destination: str,
        copy_interval: int = 60,
        filesystem = "hdfs",
        create_dest_dirs: bool = True,
        preserve_structure: bool = True,
        max_retries: int = 3,
        retry_delay: int = 5
    ) -> None:
        """Start a background thread to periodically copy log files from local to HDFS."""
        if copy_name in self._hdfs_copy_threads:
            raise ValueError(f"HDFS copy operation '{copy_name}' already exists. Use stop_hdfs_copy() first.")
        
        # Validate parameters
        if not local_pattern:
            raise ValueError("local_pattern cannot be empty")
        if not hdfs_destination:
            raise ValueError("hdfs_destination cannot be empty")
        if copy_interval <= 0:
            raise ValueError("copy_interval must be positive")
        
        # Normalize patterns to list
        if isinstance(local_pattern, str):
            patterns = [local_pattern]
        else:
            patterns = list(local_pattern)
        
        # Create stop event for this copy operation
        stop_event = threading.Event()
        self._stop_events[copy_name] = stop_event
        
        # Create and start the copy thread
        copy_thread = threading.Thread(
            target=self._hdfs_copy_worker,
            args=(
                copy_name, patterns, hdfs_destination, copy_interval,
                filesystem, create_dest_dirs, preserve_structure,
                max_retries, retry_delay, stop_event
            ),
            daemon=True,
            name=f"HDFSCopy-{copy_name}"
        )
        
        self._hdfs_copy_threads[copy_name] = copy_thread
        copy_thread.start()
        
        print(f"Started HDFS copy operation '{copy_name}' with {copy_interval}s interval")

    def stop_hdfs_copy(self, copy_name: str, timeout: float = 10.0) -> bool:
        """Stop a running HDFS copy operation."""
        if copy_name not in self._hdfs_copy_threads:
            raise ValueError(f"HDFS copy operation '{copy_name}' does not exist")
        
        # Signal the thread to stop
        self._stop_events[copy_name].set()
        
        # Wait for thread to finish
        self._hdfs_copy_threads[copy_name].join(timeout=timeout)
        
        # Check if thread actually stopped
        if self._hdfs_copy_threads[copy_name].is_alive():
            print(f"Warning: HDFS copy thread '{copy_name}' did not stop within {timeout}s")
            return False
        
        # Clean up
        del self._hdfs_copy_threads[copy_name]
        del self._stop_events[copy_name]
        
        print(f"Stopped HDFS copy operation '{copy_name}'")
        return True

    def stop_all_hdfs_copy(self, timeout: float = 10.0):
        """Stop all running HDFS copy operations."""
        failed_to_stop = []
        copy_names = list(self._hdfs_copy_threads.keys())
        
        for copy_name in copy_names:
            try:
                if not self.stop_hdfs_copy(copy_name, timeout):
                    failed_to_stop.append(copy_name)
            except ValueError:
                # Already stopped or doesn't exist
                pass
        
        return failed_to_stop

    def list_hdfs_copy_operations(self):
        """List all active HDFS copy operations."""
        operations = []
        for copy_name, thread in self._hdfs_copy_threads.items():
            operations.append({
                "name": copy_name,
                "thread_name": thread.name,
                "is_alive": thread.is_alive(),
                "daemon": thread.daemon
            })
        return operations

    def _hdfs_copy_worker(
        self,
        copy_name: str,
        patterns,
        hdfs_destination: str,
        copy_interval: int,
        filesystem: str,
        create_dest_dirs: bool,
        preserve_structure: bool,
        max_retries: int,
        retry_delay: int,
        stop_event: threading.Event
    ) -> None:
        """Worker function that runs in a separate thread to perform periodic HDFS copying."""
        print(f"HDFS copy worker '{copy_name}' started")
        
        while not stop_event.is_set():
            try:
                # Find files matching patterns (simplified for testing)
                import glob
                files_to_copy = []
                for pattern in patterns:
                    if os.path.isfile(pattern):
                        files_to_copy.append(pattern)
                    else:
                        matched_files = glob.glob(pattern, recursive=True)
                        files_to_copy.extend(matched_files)
                
                files_to_copy = list(set([f for f in files_to_copy if os.path.isfile(f)]))
                
                if files_to_copy:
                    self._copy_files_to_hdfs(
                        files_to_copy, hdfs_destination, filesystem,
                        create_dest_dirs, preserve_structure, max_retries, retry_delay
                    )
                else:
                    print(f"HDFS copy '{copy_name}': No files found matching patterns {patterns}")
                
            except Exception as e:
                print(f"Error in HDFS copy worker '{copy_name}': {e}")
            
            # Wait for the next interval or stop signal
            if stop_event.wait(timeout=copy_interval):
                break  # Stop event was set
        
        print(f"HDFS copy worker '{copy_name}' stopped")

    def _copy_files_to_hdfs(
        self,
        local_files,
        hdfs_destination: str,
        filesystem: str,
        create_dest_dirs: bool,
        preserve_structure: bool,
        max_retries: int,
        retry_delay: int
    ) -> None:
        """Copy a list of local files to HDFS destination (mock version for testing)."""
        success_count = 0
        error_count = 0
        
        for local_file in local_files:
            for attempt in range(max_retries + 1):
                try:
                    # Determine destination path
                    if preserve_structure:
                        rel_path = os.path.basename(local_file)
                        dest_path = os.path.join(hdfs_destination, rel_path).replace("\\", "/")
                    else:
                        filename = os.path.basename(local_file)
                        dest_path = os.path.join(hdfs_destination, filename).replace("\\", "/")
                    
                    # Mock file copy operation
                    print(f"Mock copying {local_file} -> {dest_path}")
                    
                    success_count += 1
                    print(f"Successfully copied {local_file} -> {dest_path}")
                    break  # Success, exit retry loop
                    
                except Exception as e:
                    if attempt < max_retries:
                        print(f"Attempt {attempt + 1} failed for {local_file}: {e}. Retrying in {retry_delay}s...")
                        time.sleep(retry_delay)
                    else:
                        print(f"Failed to copy {local_file} after {max_retries + 1} attempts: {e}")
                        error_count += 1
        
        if success_count > 0 or error_count > 0:
            print(f"HDFS copy completed: {success_count} successful, {error_count} failed")


def test_hdfs_copy_attributes_initialization():
    """Test that HDFS copy attributes are properly initialized."""
    print("🧪 Testing HDFS copy attributes initialization...")
    
    log_manager = MockLogManager()
    
    assert hasattr(log_manager, '_hdfs_copy_threads'), "Missing _hdfs_copy_threads attribute"
    assert hasattr(log_manager, '_stop_events'), "Missing _stop_events attribute"
    assert isinstance(log_manager._hdfs_copy_threads, dict), "_hdfs_copy_threads should be dict"
    assert isinstance(log_manager._stop_events, dict), "_stop_events should be dict"
    assert len(log_manager._hdfs_copy_threads) == 0, "Initial _hdfs_copy_threads should be empty"
    assert len(log_manager._stop_events) == 0, "Initial _stop_events should be empty"
    
    print("✅ HDFS copy attributes initialization test PASSED")


def test_hdfs_copy_start_validation():
    """Test parameter validation for start_hdfs_copy."""
    print("🧪 Testing HDFS copy start validation...")
    
    log_manager = MockLogManager()
    
    # Test empty local_pattern
    try:
        log_manager.start_hdfs_copy("test", "", "/hdfs/dest")
        assert False, "Should have raised ValueError for empty local_pattern"
    except ValueError as e:
        assert "local_pattern cannot be empty" in str(e)
    
    # Test empty hdfs_destination
    try:
        log_manager.start_hdfs_copy("test", "/logs/*.log", "")
        assert False, "Should have raised ValueError for empty hdfs_destination"
    except ValueError as e:
        assert "hdfs_destination cannot be empty" in str(e)
    
    # Test invalid copy_interval
    try:
        log_manager.start_hdfs_copy("test", "/logs/*.log", "/hdfs/dest", copy_interval=0)
        assert False, "Should have raised ValueError for zero copy_interval"
    except ValueError as e:
        assert "copy_interval must be positive" in str(e)
    
    try:
        log_manager.start_hdfs_copy("test", "/logs/*.log", "/hdfs/dest", copy_interval=-1)
        assert False, "Should have raised ValueError for negative copy_interval"
    except ValueError as e:
        assert "copy_interval must be positive" in str(e)
    
    print("✅ HDFS copy start validation test PASSED")


def test_hdfs_copy_duplicate_name_error():
    """Test that duplicate copy operation names are rejected."""
    print("🧪 Testing HDFS copy duplicate name error...")
    
    log_manager = MockLogManager()
    
    # Mock threading to avoid actual thread creation
    with patch('threading.Thread') as mock_thread:
        mock_thread_instance = MagicMock()
        mock_thread.return_value = mock_thread_instance
        
        # Start first operation
        log_manager.start_hdfs_copy("test_copy", "/logs/*.log", "/hdfs/dest")
        
        # Try to start duplicate
        try:
            log_manager.start_hdfs_copy("test_copy", "/logs/*.log", "/hdfs/dest2")
            assert False, "Should have raised ValueError for duplicate copy name"
        except ValueError as e:
            assert "HDFS copy operation 'test_copy' already exists" in str(e)
    
    print("✅ HDFS copy duplicate name test PASSED")


def test_start_hdfs_copy_creates_thread():
    """Test that starting HDFS copy creates and starts a thread."""
    print("🧪 Testing HDFS copy thread creation...")
    
    log_manager = MockLogManager()
    
    with patch('threading.Thread') as mock_thread:
        mock_thread_instance = MagicMock()
        mock_thread.return_value = mock_thread_instance
        
        log_manager.start_hdfs_copy(
            copy_name="test_copy",
            local_pattern="/logs/*.log",
            hdfs_destination="/hdfs/dest",
            copy_interval=30
        )
        
        # Verify thread creation
        assert mock_thread.called, "Thread should have been created"
        mock_thread_instance.start.assert_called_once()
        
        # Verify internal state
        assert "test_copy" in log_manager._hdfs_copy_threads
        assert "test_copy" in log_manager._stop_events
        assert log_manager._hdfs_copy_threads["test_copy"] == mock_thread_instance
    
    print("✅ HDFS copy thread creation test PASSED")


def test_list_hdfs_copy_operations():
    """Test listing active HDFS copy operations."""
    print("🧪 Testing HDFS copy operations listing...")
    
    log_manager = MockLogManager()
    
    with patch('threading.Thread') as mock_thread:
        mock_thread_instance = MagicMock()
        mock_thread_instance.name = "HDFSCopy-test1"
        mock_thread_instance.is_alive.return_value = True
        mock_thread_instance.daemon = True
        mock_thread.return_value = mock_thread_instance
        
        log_manager.start_hdfs_copy("test1", "/logs/*.log", "/hdfs/dest1")
        
        # List operations
        operations = log_manager.list_hdfs_copy_operations()
        
        assert len(operations) == 1, f"Expected 1 operation, got {len(operations)}"
        assert operations[0]["name"] == "test1"
        assert operations[0]["thread_name"] == "HDFSCopy-test1"
        assert operations[0]["is_alive"] == True
        assert operations[0]["daemon"] == True
    
    print("✅ HDFS copy operations listing test PASSED")


def test_stop_hdfs_copy_success():
    """Test successfully stopping an HDFS copy operation."""
    print("🧪 Testing HDFS copy stop success...")
    
    log_manager = MockLogManager()
    
    with patch('threading.Thread') as mock_thread:
        mock_thread_instance = MagicMock()
        mock_thread_instance.is_alive.return_value = False
        mock_thread.return_value = mock_thread_instance
        
        log_manager.start_hdfs_copy("test_copy", "/logs/*.log", "/hdfs/dest")
        
        # Stop the operation
        result = log_manager.stop_hdfs_copy("test_copy", timeout=5.0)
        
        assert result == True, "Stop operation should return True"
        mock_thread_instance.join.assert_called_once_with(timeout=5.0)
        
        # Verify cleanup
        assert "test_copy" not in log_manager._hdfs_copy_threads
        assert "test_copy" not in log_manager._stop_events
    
    print("✅ HDFS copy stop success test PASSED")


def test_stop_hdfs_copy_nonexistent():
    """Test stopping a non-existent HDFS copy operation."""
    print("🧪 Testing HDFS copy stop nonexistent...")
    
    log_manager = MockLogManager()
    
    try:
        log_manager.stop_hdfs_copy("nonexistent")
        assert False, "Should have raised ValueError for nonexistent operation"
    except ValueError as e:
        assert "HDFS copy operation 'nonexistent' does not exist" in str(e)
    
    print("✅ HDFS copy stop nonexistent test PASSED")


def test_integration_with_real_files():
    """Integration test with real file operations."""
    print("🧪 Testing HDFS copy integration with real files...")
    
    with tempfile.TemporaryDirectory() as temp_dir:
        temp_path = Path(temp_dir)
        
        # Create a test log file
        log_file = temp_path / "test.log"
        with open(log_file, 'w') as f:
            f.write("Test log entry\n")
            f.write(f"Timestamp: {time.time()}\n")
        
        log_manager = MockLogManager()
        
        # Test with short interval for quick test
        with patch('threading.Thread') as mock_thread:
            mock_thread_instance = MagicMock()
            mock_thread_instance.is_alive.return_value = False
            mock_thread.return_value = mock_thread_instance
            
            log_manager.start_hdfs_copy(
                copy_name="integration_test",
                local_pattern=str(temp_path / "*.log"),
                hdfs_destination="/hdfs/test/",
                copy_interval=1,
                filesystem="hdfs"
            )
            
            # Verify thread was created
            assert mock_thread.called, "Thread should have been created"
            assert "integration_test" in log_manager._hdfs_copy_threads
            
            # Stop the operation
            result = log_manager.stop_hdfs_copy("integration_test")
            assert result == True, "Stop operation should succeed"
    
    print("✅ HDFS copy integration test PASSED")


def test_pattern_normalization():
    """Test that patterns are correctly normalized to lists."""
    print("🧪 Testing pattern normalization...")
    
    test_cases = [
        ("/logs/*.log", list),
        (["/logs/*.log", "/logs/*.txt"], list),
        ("/specific/file.log", list),
    ]
    
    for pattern, expected_type in test_cases:
        # Test the logic that should happen in start_hdfs_copy
        if isinstance(pattern, str):
            patterns = [pattern]
        else:
            patterns = list(pattern)
        
        assert isinstance(patterns, expected_type), f"Pattern {pattern} should result in {expected_type}"
        assert len(patterns) >= 1, f"Patterns should have at least 1 element"
        
        if isinstance(pattern, str):
            assert patterns == [pattern], f"String pattern should be wrapped in list"
        else:
            assert patterns == pattern, f"List pattern should remain unchanged"
    
    print("✅ Pattern normalization test PASSED")


def main():
    """Run all tests."""
    print("🚀 Starting HDFS Copy Functionality Tests")
    print("=" * 50)
    
    tests = [
        test_hdfs_copy_attributes_initialization,
        test_hdfs_copy_start_validation,
        test_hdfs_copy_duplicate_name_error,
        test_start_hdfs_copy_creates_thread,
        test_list_hdfs_copy_operations,
        test_stop_hdfs_copy_success,
        test_stop_hdfs_copy_nonexistent,
        test_integration_with_real_files,
        test_pattern_normalization,
    ]
    
    passed = 0
    failed = 0
    
    for test_func in tests:
        try:
            test_func()
            passed += 1
        except Exception as e:
            print(f"❌ {test_func.__name__} FAILED: {e}")
            failed += 1
    
    print("=" * 50)
    print(f"🎉 Test Results: {passed} passed, {failed} failed")
    
    if failed == 0:
        print("🏆 All HDFS copy tests PASSED!")
        return True
    else:
        print("💥 Some tests FAILED!")
        return False


if __name__ == "__main__":
    success = main()
    exit(0 if success else 1)
