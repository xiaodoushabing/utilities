import pytest

pytestmark = pytest.mark.integration

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
