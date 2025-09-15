"""
LogManager - Main module containing the LogManager class.

This module provides the core LogManager class for task-aware logging
built on top of Loguru. The LogManager now uses composition to separate
concerns into specialized components:
- LoggingManager: Handles Loguru configuration and logger management
- CopyManager: Handles background file copying operations
- DistributedCoordinator: Handles distributed system coordination
"""

import atexit
from typing import Optional, List, Union

from ._logging_manager import LoggingManager
from ._copy_manager import CopyManager
from ._distributed_coordinator import DistributedCoordinator

# Make component managers available at package level for easier imports
__all__ = [
    "LogManager",
    "LoggingManager", 
    "CopyManager",
    "DistributedCoordinator"
]

class LogManager:
    """
    LogManager class to manage logging configuration and copy operations.
    
    This class provides a centralized way to manage Loguru loggers and handlers
    using YAML configuration files, along with file copying capabilities.
    
    The LogManager now uses composition to separate concerns:
    - LoggingManager: Handles Loguru configuration and logger management
    - CopyManager: Handles background file copying operations
    - DistributedCoordinator: Handles distributed system coordination
    
    This design provides better maintainability, testability, and separation of concerns
    while maintaining full backward compatibility with existing code.
    """

    def __init__(
            self,
            config_path: Optional[str] = None,
            timezone: str = "Asia/Singapore",
    ):
        """
        Initialize LogManager with a configuration file path and timezone.
        
        Args:
            config_path (Optional[str]): Path to the configuration file. Defaults to None, which uses the default config file.
            timezone (str): Timezone to set for logging. Default is "Asia/Singapore".
        """
        # Initialize distributed coordination first
        self._coordinator = DistributedCoordinator()
        
        # Initialize logging manager
        self._logging_manager = LoggingManager(config_path=config_path, timezone=timezone)
        
        # Initialize copy manager (enabled based on distributed coordination)
        self._copy_manager = CopyManager(
            config=self.config.get("copy_manager", {}),
            retry=self.config.get("retry",{}),
            enabled=self._coordinator.copy_enabled
        )

        # Register cleanup on exit
        atexit.register(self._cleanup)
    
    # ========================================================================================
    # LOGGING METHODS - Delegate to LoggingManager  
    # ========================================================================================
    
    @property
    def config(self):
        return self._logging_manager.config
    
    @property
    def _config_path(self):
        return self._logging_manager._config_path

    @property 
    def _handlers_map(self):
        return self._logging_manager._handlers_map
        
    @property
    def _loggers_map(self):
        return self._logging_manager._loggers_map
    
    def get_logger(self, logger_name: str):
        """
        Retrieve a logger instance bound to the specified logger name.

        Args:
            logger_name (str): The name of the logger to retrieve.

        Returns:
            Logger: A logger instance bound to the specified logger name.

        Raises:
            AssertionError: If the logger with the specified name does not exist.
                Use add_logger() to create new loggers.
        """
        return self._logging_manager.get_logger(logger_name)
    
    def add_logger(self, logger_name: str, handlers: list[tuple[str, dict]]):
        """
        Add a new logger with the specified name and handlers.

        Args:
            logger_name (str): The name of the logger to add.
            handlers (list[tuple[str, dict]]): A list of tuples where each tuple contains 
                a handler name and its configuration. The handler configuration should be 
                a dictionary with keys like 'level', 'format', etc.

        Raises:
            AssertionError: If the logger with the specified name already exists.
                Use update_logger() to modify existing loggers.
        """
        return self._logging_manager.add_logger(logger_name, handlers)

    def update_logger(self, logger_name: str, handlers: list[tuple[str, dict]]):
        """
        Update an existing logger with the specified name and handlers.

        Args:
            logger_name (str): The name of the logger to update.
            handlers (list[tuple[str, dict]]): A list of tuples where each tuple contains 
                a handler name and its configuration. The handler configuration should be 
                a dictionary with keys like 'level', 'format', etc.
        
        Raises:
            AssertionError: If the logger with the specified name does not exist.
                Use add_logger() to create new loggers.
        """
        return self._logging_manager.update_logger(logger_name, handlers)

    def remove_logger(self, logger_name: str):
        """
        Remove a logger by its name.
        
        Args:
            logger_name (str): The name of the logger to remove.
        
        Raises:
            AssertionError: If the logger with the specified name does not exist.
            
        Note:
            This will also clean up all handler mappings associated with the logger.
        """
        return self._logging_manager.remove_logger(logger_name)

    def add_handler(self, handler_name: str, handler_conf: dict):
        """
        Configure and add a new handler to the logger.

        Args:
            handler_name (str): The unique name of the handler to add.
            handler_conf (dict): The configuration dictionary for the handler.

        Raises:
            AssertionError: If the handler with the given name already exists.
                Use update_handler() to modify existing handlers.
        """
        return self._logging_manager.add_handler(handler_name, handler_conf)

    def update_handler(self, handler_name: str, handler_conf: dict):
        """
        Update an existing handler with the specified configuration.
        
        Args:
            handler_name (str): The name of the handler to update.
            handler_conf (dict): The new configuration for the handler.

        Raises:
            AssertionError: If the handler with the given name does not exist.
                Use add_handler() to create new handlers.
        """
        return self._logging_manager.update_handler(handler_name, handler_conf)

    def remove_handler(self, handler_name: str):
        """
        Remove a handler by its name.

        Args:
            handler_name (str): The name of the handler to remove.
            
        Raises:
            AssertionError: If the handler with the given name does not exist.
            
        Note:
            This will also clean up all logger mappings associated with the handler.
        """
        return self._logging_manager.remove_handler(handler_name)
    
    
    # ========================================================================================
    # DISTRIBUTED COORDINATION METHODS - Delegate to DistributedCoordinator
    # ========================================================================================

    @property
    def copy_enabled(self):
        """Access to copy enabled status for backward compatibility."""
        return self._coordinator.copy_enabled
    
    def get_copy_status(self) -> dict:
        """
        Get current copy status and environment information.
        
        Returns:
            dict: Status information including enabled state and reason.
        """
        return self._coordinator.get_copy_status()


    # ========================================================================================
    # COPY METHODS - Delegate to CopyManager
    # ========================================================================================
    def start_copy_from_config(
            self,
            config: dict = None
    ) -> None:
        """
        Start all copy operations based on the provided configuration.

        Args:
            config (dict): Configuration dictionary for the copy operation. Defaults to None.
        """
        self._copy_manager.start_copy_from_config(config)

    def start_copy(
        self,
        copy_name: str = None,
        path_patterns: str = None,
        copy_destination: str = None,
        root_dir: Optional[str] = None,
        copy_interval: Optional[int] = None,
        create_dest_dirs: Optional[bool] = None,
        preserve_structure: Optional[bool] = None,
        max_retries: Optional[int] = None,
        retry_delay: Optional[int] = None
    ) -> None:
        """
        Start a background thread to periodically copy log files from local to destination.
        
        Args:
            copy_name (str): Unique name for this copy operation (used for thread identification).
            path_patterns (List[str]): Glob pattern(s) or file path(s) to match local files.
                Examples: 
                - ["/path/to/logs/*.log"]
                - ["/path/to/logs/*.log", "/path/to/logs/*.txt"]
                - ["/path/to/specific/file.log"]
            copy_destination (str): Destination directory path.
                Example: "hdfs://namenode:port/path/to/hdfs/logs/"
            root_dir (Optional[str]): Root directory for the local files.
            copy_interval (Optional[int]): Interval in seconds between copy operations. Default is 60 seconds.
            create_dest_dirs (Optional[bool]): Whether to create destination directories if they don't exist.
            preserve_structure (Optional[bool]): Whether to preserve local directory structure in destination.
                If True, root_dir must be specified.
                If True: "/local/logs/app/file.log" -> "hdfs://dest/app/file.log"
                If False: "/local/logs/app/file.log" -> "hdfs://dest/file.log"
            max_retries (Optional[int]): Maximum number of retry attempts for failed copies. Default is 3.
            retry_delay (Optional[int]): Delay in seconds between retry attempts. Default is 5.

        Raises:
            ValueError: If copy_name already exists or parameters are invalid.
        """
        all_kwargs = {
            "copy_name": copy_name,
            "path_patterns": path_patterns,
            "copy_destination": copy_destination,
            "root_dir": root_dir,
            "copy_interval": copy_interval,
            "create_dest_dirs": create_dest_dirs,
            "preserve_structure": preserve_structure,
            "max_retries": max_retries,
            "retry_delay": retry_delay
        }

        kwargs = {k: v for k, v in all_kwargs.items() if v is not None}
        
        return self._copy_manager.start_copy(**kwargs)

    def stop_copy(self, copy_name: str, timeout: float = None) -> bool:
        """
        Stop a running copy operation.
        
        Args:
            copy_name (str): Name of the copy operation to stop.
            timeout (float): Maximum time to wait for thread to stop. Defaults to None.
            
        Returns:
            bool: True if successfully stopped, False if timeout occurred.
            
        Raises:
            ValueError: If copy_name doesn't exist.
        """
        all_kwargs = {
            "copy_name": copy_name,
            "timeout": timeout
        }

        kwargs = {k: v for k, v in all_kwargs.items() if v is not None}
        return self._copy_manager.stop_copy(**kwargs)

    def stop_all_copy(self, timeout: float = None, verbose: bool = None) -> List[str]:
        """
        Stop all running copy operations.
        
        Args:
            timeout (float): Maximum time to wait for each thread to stop. Defaults to 30 seconds.
            verbose (bool): Whether to provide detailed feedback during the operation. Defaults to False.
            
        Returns:
            List[str]: Names of copy operations that failed to stop within timeout.
        """
        all_kwargs = {
            "timeout": timeout,
            "verbose": verbose
        }

        kwargs = {k: v for k, v in all_kwargs.items() if v is not None}
        return self._copy_manager.stop_all_copy_operations(**kwargs)

    def list_copy_operations(self) -> List[dict]:
        """
        List all active copy operations.
        
        Returns:
            List[dict]: Information about active copy operations.
        """
        return self._copy_manager.list_copy_operations()
    
    def trigger_copy_now(self, copy_name: Optional[Union[str, List[str]]]  = None) -> None:
        """
        Manually trigger an immediate copy operation for specific or all copy operations.
        
        This method allows you to force a copy operation outside of the normal
        interval schedule. Useful for ensuring log files are copied before
        critical operations or during testing.
        
        Args:
            copy_name (Optional[str]): Name of specific copy operation to trigger.
                                     If None, triggers all active operations.
                                     
        Raises:
            ValueError: If copy_name is specified but doesn't exist.
        """
        return self._copy_manager.trigger_copy_now(copy_name)


    # ========================================================================================
    # CLEANUP METHODS
    # ========================================================================================

    def _cleanup(self, timeout: float = None):
        """
        Cleanup function to remove all handlers and loggers.
        
        This method is automatically called on program exit via atexit.register().
        It ensures proper cleanup of all Loguru handlers and clears the internal
        mapping dictionaries. It also performs a final copy operation for all
        active copy operations before stopping them.
        
        Args:
            timeout (float): Timeout in seconds for stopping copy operations.
                                    Defaults to 60.0.
        
        Note: This method can be called multiple times safely.
        """
        print("LogManager cleanup initiated...")
        
        # Cleanup copy operations first (includes final copy)
        
        kwargs = {"timeout": timeout} if timeout is not None else {}
        self._copy_manager.cleanup(**kwargs)

        # Cleanup logging
        self._logging_manager.cleanup()
        
        print("LogManager cleanup completed.")
