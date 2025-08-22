"""
Signal Handling Tests for LogManager
"""

import pytest
import signal
import os
from unittest.mock import patch, MagicMock, call

from utilities.logger import LogManager

pytestmark = pytest.mark.unit

@patch('utilities.logger.signal.signal')
@patch('utilities.logger.hasattr')
@patch('builtins.print')
class TestSignalHandlerSetup:
    """Test signal handler registration and setup."""
    
    def test_signal_handlers_registered_successfully(self, mock_print, mock_hasattr, mock_signal_signal, mock_logger):
        """Test that signal handlers are registered when SIGTERM is available."""
        mock_hasattr.return_value = True
        mock_signal_signal.return_value = None  # Successful registration
        
        # Create a new LogManager to trigger signal handler setup
        manager = LogManager()
        
        # Verify signal.signal was called for both SIGINT and SIGTERM
        assert mock_signal_signal.call_count == 2
        mock_print.assert_any_call("Signal handlers registered")

    @pytest.mark.parametrize("exception_type", [
        OSError,
        ValueError,
        RuntimeError
    ])
    def test_signal_handlers_registration_fails_gracefully(self, mock_print, mock_hasattr, mock_signal_signal, exception_type, mock_logger):
        """Test graceful handling when signal registration fails."""
        mock_hasattr.return_value = True
        mock_signal_signal.side_effect = exception_type("Signal error")

        # Create a new LogManager to trigger signal handler setup
        manager = LogManager()
        
        # Verify error message is printed and program continues
        mock_print.assert_any_call(
            "Could not register signal handlers: Signal error.\n"
            "LogManager will rely on atexit for cleanup instead."
        )
    
    def test_signal_handlers_skipped_when_sigterm_unavailable(self, mock_print, mock_hasattr, mock_signal_signal, mock_logger):
        """Test that signal registration is skipped when SIGTERM is not available."""
        mock_hasattr.return_value = False
        
        # Create a new LogManager to trigger signal handler setup
        manager = LogManager()
        
        # Verify signal.signal was never called
        mock_signal_signal.assert_not_called()

@patch('utilities.logger.os.kill')
@patch('utilities.logger.signal.signal')
class TestSignalHandlerBehavior:
    """Test signal handler behavior when signals are received."""

    @pytest.mark.parametrize("sig", [
        signal.SIGINT,
        signal.SIGTERM
    ])
    @patch('builtins.print')
    @patch('utilities.logger.LogManager._cleanup')
    def test_signal_handler_calls_cleanup_and_reraises(self, mock_cleanup, mock_print, mock_signal_signal, mock_os_kill, mock_logger, log_manager, sig):
        """Test that signal handler performs cleanup and re-raises signal."""
        # Mock the cleanup method
        log_manager._setup_signal_handlers()

        # Extract the signal handler from the mock calls
        signal_handler = mock_signal_signal.call_args_list[0][0][1]
        
        # Simulate receiving SIGINT (signal number 2)
        signal_handler(sig, None)
        
        # Verify shutdown message was printed
        mock_print.assert_any_call(
            f"\nReceived signal '{sig}'. Stopping all HDFS copy operations and cleaning up..."
        )

        # Verify cleanup was called with correct timeout
        mock_cleanup.assert_called_once()
        # Verify signal was restored to default and re-raised
        mock_signal_signal.assert_any_call(sig, signal.SIG_DFL)
        mock_os_kill.assert_called_once_with(os.getpid(), sig)


    @patch('builtins.print')
    def test_signal_handler_stops_hdfs_operations(self, mock_print, mock_signal_signal, mock_os_kill, log_manager, hdfs_copy_defaults):
        """Test that signal handler properly stops HDFS operations."""
        # Start an HDFS copy operation
        log_manager.start_hdfs_copy(**hdfs_copy_defaults)
        
        # Verify operation is running
        assert len(log_manager._hdfs_copy_threads) == 1
        
        # Get the signal handler and simulate signal
        log_manager._setup_signal_handlers()
        signal_handler = mock_signal_signal.call_args_list[0][0][1]
        
        # Simulate receiving SIGINT
        signal_handler(signal.SIGINT, None)
        
        # Verify HDFS operations were stopped
        assert len(log_manager._hdfs_copy_threads) == 0
        assert len(log_manager._stop_events) == 0
        
        # Verify signal re-raising occurred
        mock_os_kill.assert_called_once_with(os.getpid(), signal.SIGINT)
    

    def test_signal_handler_prevents_new_hdfs_operations(self, mock_signal_signal, mock_os_kill, mock_logger, log_manager, hdfs_copy_defaults):
        """Test that signal handler prevents new HDFS operations after cleanup."""
        # Get the signal handler and simulate signal
        log_manager._setup_signal_handlers()
        signal_handler = mock_signal_signal.call_args_list[0][0][1]
        
        # Simulate receiving SIGINT (this sets _shutdown_in_progress = True)
        signal_handler(signal.SIGINT, None)
        
        # Verify shutdown flag is set
        assert log_manager._shutdown_in_progress is True
        
        # Try to start a new HDFS operation - should fail
        with pytest.raises(ValueError, match="LogManager is shutting down"):
            log_manager.start_hdfs_copy(**hdfs_copy_defaults)


@patch('utilities.logger.os.kill')
@patch('utilities.logger.signal.signal')
class TestSignalHandlerEdgeCases:
    """Test edge cases and error conditions in signal handling."""

    def test_multiple_signals_handled_gracefully(self, mock_signal_signal, mock_os_kill, mock_logger, log_manager):
        """Test that multiple signals in quick succession are handled properly."""
        # Setup signal handler
        log_manager._setup_signal_handlers()
        signal_handler = mock_signal_signal.call_args_list[0][0][1]
        
        # Simulate receiving first signal
        signal_handler(signal.SIGINT, None)
        
        # Reset mock for second signal
        mock_os_kill.reset_mock()
        
        # Simulate receiving second signal - should be handled gracefully
        signal_handler(signal.SIGTERM, None)
        
        # Verify second signal was also re-raised
        mock_os_kill.assert_called_once_with(os.getpid(), signal.SIGTERM)

    @patch('utilities.logger.LogManager._cleanup')
    def test_signal_handler_cleanup_exception_handling(self, mock_cleanup, mock_signal_signal, mock_os_kill, mock_logger, log_manager):
        """Test that signal handler propagates cleanup exceptions."""
        # Make cleanup raise an exception
        mock_cleanup.side_effect = Exception("Cleanup failed")
        
        # Setup signal handler
        log_manager._setup_signal_handlers()
        signal_handler = mock_signal_signal.call_args_list[0][0][1]
        
        # Simulate receiving signal - should raise the cleanup exception
        with pytest.raises(Exception, match="Cleanup failed"):
            signal_handler(signal.SIGINT, None)
        
        # Verify cleanup was attempted
        mock_cleanup.assert_called_once()
        
        # Verify signal restoration and re-raising didn't occur due to exception
        mock_os_kill.assert_not_called()
