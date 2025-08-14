"""
Signal Handling Tests for LogManager
"""

import pytest
import signal
import os
from unittest.mock import patch, MagicMock, call

pytestmark = pytest.mark.unit


class TestSignalHandlerSetup:
    """Test signal handler registration and setup."""
    
    @patch('utilities.logger.signal.signal')
    @patch('utilities.logger.hasattr')
    @patch('builtins.print')
    def test_signal_handlers_registered_successfully(self, mock_print, mock_hasattr, mock_signal_signal, log_manager):
        """Test that signal handlers are registered when SIGTERM is available."""
        mock_hasattr.return_value = True
        mock_signal_signal.return_value = None  # Successful registration
        
        # Create a new LogManager to trigger signal handler setup
        from utilities.logger import LogManager
        manager = LogManager()
        
        # Verify signal.signal was called for both SIGINT and SIGTERM
        expected_calls = [
            call(signal.SIGINT, mock_signal_signal.call_args_list[0][0][1]),
            call(signal.SIGTERM, mock_signal_signal.call_args_list[1][0][1])
        ]
        assert mock_signal_signal.call_count == 2
        mock_print.assert_any_call("Signal handlers registered")
    
    @patch('utilities.logger.signal.signal')
    @patch('utilities.logger.hasattr')
    @patch('builtins.print')
    def test_signal_handlers_registration_fails_gracefully(self, mock_print, mock_hasattr, mock_signal_signal, log_manager):
        """Test graceful handling when signal registration fails."""
        mock_hasattr.return_value = True
        mock_signal_signal.side_effect = OSError("Signal registration failed")
        
        # Create a new LogManager to trigger signal handler setup
        from utilities.logger import LogManager
        manager = LogManager()
        
        # Verify error message is printed and program continues
        mock_print.assert_any_call(
            "Could not register signal handlers: Signal registration failed.\n"
            "LogManager will rely on atexit for cleanup instead."
        )
    
    @patch('utilities.logger.signal.signal')
    @patch('utilities.logger.hasattr')
    @patch('builtins.print')
    def test_signal_handlers_skipped_when_sigterm_unavailable(self, mock_print, mock_hasattr, mock_signal_signal, log_manager):
        """Test that signal registration is skipped when SIGTERM is not available."""
        mock_hasattr.return_value = False
        
        # Create a new LogManager to trigger signal handler setup
        from utilities.logger import LogManager
        manager = LogManager()
        
        # Verify signal.signal was never called
        mock_signal_signal.assert_not_called()


class TestSignalHandlerBehavior:
    """Test signal handler behavior when signals are received."""
    
    @patch('utilities.logger.os.kill')
    @patch('utilities.logger.signal.signal')
    @patch('builtins.print')
    def test_signal_handler_calls_cleanup_and_reraises(self, mock_print, mock_signal_signal, mock_os_kill, log_manager):
        """Test that signal handler performs cleanup and re-raises signal."""
        # Mock the cleanup method
        with patch.object(log_manager, '_cleanup') as mock_cleanup:
            # Get the signal handler function that was registered
            log_manager._setup_signal_handlers()
            
            # Extract the signal handler from the mock calls
            signal_handler = mock_signal_signal.call_args_list[0][0][1]
            
            # Simulate receiving SIGINT (signal number 2)
            signal_handler(signal.SIGINT, None)
            
            # Verify cleanup was called with correct timeout
            mock_cleanup.assert_called_once_with(hdfs_timeout=60.0)
            
            # Verify signal was restored to default and re-raised
            mock_signal_signal.assert_any_call(signal.SIGINT, signal.SIG_DFL)
            mock_os_kill.assert_called_once_with(os.getpid(), signal.SIGINT)
            
            # Verify shutdown message was printed
            mock_print.assert_any_call(
                f"\nReceived signal '{signal.SIGINT}'. Stopping all HDFS copy operations and cleaning up..."
            )
    
    @patch('utilities.logger.os.kill')
    @patch('utilities.logger.signal.signal')
    @patch('builtins.print')
    def test_signal_handler_works_for_sigterm(self, mock_print, mock_signal_signal, mock_os_kill, log_manager):
        """Test that signal handler works correctly for SIGTERM."""
        # Mock the cleanup method
        with patch.object(log_manager, '_cleanup') as mock_cleanup:
            # Get the signal handler function that was registered
            log_manager._setup_signal_handlers()
            
            # Extract the signal handler from the mock calls (should be same for both SIGINT and SIGTERM)
            signal_handler = mock_signal_signal.call_args_list[1][0][1]
            
            # Simulate receiving SIGTERM (signal number 15)
            signal_handler(signal.SIGTERM, None)
            
            # Verify cleanup was called
            mock_cleanup.assert_called_once_with(hdfs_timeout=60.0)
            
            # Verify signal was restored and re-raised with correct signal number
            mock_signal_signal.assert_any_call(signal.SIGTERM, signal.SIG_DFL)
            mock_os_kill.assert_called_once_with(os.getpid(), signal.SIGTERM)
            
            # Verify shutdown message was printed with correct signal number
            mock_print.assert_any_call(
                f"\nReceived signal '{signal.SIGTERM}'. Stopping all HDFS copy operations and cleaning up..."
            )


class TestSignalHandlerIntegration:
    """Test signal handler integration with cleanup functionality."""
    
    @patch('utilities.logger.os.kill')
    @patch('utilities.logger.signal.signal')
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
    
    @patch('utilities.logger.os.kill')
    @patch('utilities.logger.signal.signal')
    def test_signal_handler_prevents_new_hdfs_operations(self, mock_signal_signal, mock_os_kill, log_manager, hdfs_copy_defaults):
        """Test that signal handler prevents new HDFS operations after cleanup."""
        # Get the signal handler and simulate signal
        log_manager._setup_signal_handlers()
        signal_handler = mock_signal_signal.call_args_list[0][0][1]
        
        # Simulate receiving SIGINT (this sets _shutdown_in_progress = True)
        signal_handler(signal.SIGINT, None)
        
        # Try to start a new HDFS operation - should fail
        with pytest.raises(ValueError, match="LogManager is shutting down"):
            log_manager.start_hdfs_copy(**hdfs_copy_defaults)


class TestErrorHandling:
    """Test error handling in signal setup."""
    
    @pytest.mark.parametrize("exception_type", [OSError, ValueError, RuntimeError])
    @patch('utilities.logger.signal.signal')
    @patch('utilities.logger.hasattr')
    @patch('builtins.print')
    def test_various_signal_registration_exceptions_handled(self, mock_print, mock_hasattr, mock_signal_signal, exception_type, log_manager):
        """Test that various signal registration exceptions are handled gracefully."""
        mock_hasattr.return_value = True
        mock_signal_signal.side_effect = exception_type("Signal error")
        
        # Create a new LogManager to trigger signal handler setup
        from utilities.logger import LogManager
        manager = LogManager()
        
        # Verify error message is printed
        mock_print.assert_any_call(
            "Could not register signal handlers: Signal error.\n"
            "LogManager will rely on atexit for cleanup instead."
        )
    
    @patch('utilities.logger.signal.signal')
    @patch('utilities.logger.hasattr')
    @patch('builtins.print')
    def test_unexpected_exception_in_signal_registration(self, mock_print, mock_hasattr, mock_signal_signal, log_manager):
        """Test that unexpected exceptions in signal registration are caught."""
        mock_hasattr.return_value = True
        mock_signal_signal.side_effect = Exception("Unexpected error")
        
        # Create a new LogManager to trigger signal handler setup
        from utilities.logger import LogManager
        manager = LogManager()
        
        # Verify error message is printed
        mock_print.assert_any_call(
            "Could not register signal handlers: Unexpected error.\n"
            "LogManager will rely on atexit for cleanup instead."
        )
