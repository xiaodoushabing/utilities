"""
LogManager - Main module containing the LogManager class.

This module provides the core LogManager class for task-aware logging
built on top of Loguru.
"""

import atexit
import os
import sys
import yaml
import glob
import threading
import time
import signal
from pathlib import Path
from typing import Optional, List, Union
from collections import defaultdict

from loguru import logger
from ..file_io import FileIOInterface

class LogManager:
    """
    LogManager class to manage logging configuration and handlers.
    
    This class provides a centralized way to manage Loguru loggers and handlers
    using YAML configuration files. It supports dynamic handler and logger
    management with automatic cleanup on exit.
    """
    DEFAULT_CONFIG_PATH = Path(__file__).parent / "_default_logger_config.yaml"     # Path to the default config file

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
        # update timezone
        os.environ["TZ"] = timezone
        # time.tzset()

        # logger mappers
        self._handlers_map = defaultdict(dict)  # handler_name -> {id: handler_id, loggers: {logger_name: level}}
        self._loggers_map = defaultdict(dict)   # logger_name -> [(handler_name, level), ...]

        # _handlers_map = {
        #     "handler_console": {
        #         "id": "123",
        #         "loggers": {
        #             "logger_a": {"level": "DEBUG"},
        #             "logger_b": {"level": "INFO"}
        #         }
        #     }
        # }

        # _loggers_map = {
        #     "logger_a": [
        #         {'handler': 'handler_console', 'level': 'DEBUG'},
        #         {'handler': 'handler_file', 'level': 'INFO'}
        #     ]
        # }
        
        # setup logger
        logger.remove()  # remove default logger
        self._config_path = config_path         # empty config_path is handled in _setup_logger
        self.config = {}
        self._setup_logger()

        # HDFS copy management
        self._hdfs_copy_threads = {}            # thread_name -> thread object
        self._stop_events = {}                  # thread_name -> threading.Event
        self._copy_operations_files = {}        # copy_name -> set of files being copied
        self._copy_operations_params = {}       # copy_name -> copy parameters dict
        self._shutdown_in_progress = False      # flag to prevent new copy operations during shutdown
        self._setup_signal_handlers()           # setup signal handlers for graceful shutdown

        # teardown
        atexit.register(self._cleanup)
    
    ## ------------------------------ SETUP SIGNAL HANDLERS ------------------------------ ##

    def _setup_signal_handlers(self):
        """Setup signal handlers for graceful shutdown."""
        def signal_handler(signum, frame):
            """Modify signal handler to perform cleanup."""
            print(f"\nReceived signal '{signum}'. Stopping all HDFS copy operations and cleaning up...")
            self._cleanup(hdfs_timeout=60.0)
            # restore default handler and re-raise signal
            signal.signal(signum, signal.SIG_DFL)
            os.kill(os.getpid(), signum)
        
        # register modified handler for common termination signals
        if hasattr(signal, 'SIGTERM'):
            try:
                signal.signal(signal.SIGINT, signal_handler)
                signal.signal(signal.SIGTERM, signal_handler)
                print("Signal handlers registered")
            except Exception as e:
                print(
                    f"Could not register signal handlers: {e}.\n"
                    f"LogManager will rely on atexit for cleanup instead."
                )

    ## ------------------------------ SETUP LOGGER ------------------------------ ##
    def _setup_logger(self):
        """
        Setup logger with the provided configuration file or default configuration.

        This method attempts to load the configuration from the path specified by `self._config_path`.
        If the file does not exist or is not provided, it falls back to using the class default configuration path (`self.DEFAULT_CONFIG_PATH`).
        """
        if not self._config_path:
            self._config_path = self.DEFAULT_CONFIG_PATH
            print(f"Config file not provided, initializing logger with class default config.")
        elif (not os.path.exists(self._config_path) or
                not os.path.isfile(self._config_path)):
                self._config_path = self.DEFAULT_CONFIG_PATH
                print(f"Config file {self._config_path} does not exist or is not a file, initializing logger with class default config.")
            
        logger.configure(
            extra = {}
        )

        # load config file
        with open(self._config_path, "r") as file:
            self.config = yaml.safe_load(file)
        
        # load handlers from config
        self._load_handlers(self.config)
    
    def _load_handlers(self, conf: dict):
        """
        Load handlers and loggers from the provided configuration dictionary.
        
        Args:
            conf (dict): Configuration dictionary containing handlers and loggers.

        This method clears any existing handlers and loggers stored in
        `self._handlers_map` and `self._loggers_map`, then iterates over
        the handlers and loggers defined in the configuration file to add them
        via the `_add_handler` and `_add_logger` methods.

        Raises:
            AssertionError: If 'format' is undefined or not found in the formats 
                section of the configuration file.
        """

        self._handlers_map.clear()
        self._loggers_map.clear()

        for _handler_name, _handler_conf in conf.get("handlers", {}).items():
            # load and configure handler config, and add handler to logger
            self.add_handler(_handler_name, _handler_conf)
        
        for _logger_name, _handlers in conf.get("loggers", {}).items():
            # load and configure logger config, and add logger to logger
            self.add_logger(_logger_name, _handlers)
    
    ## ------------------------------ HANDLER MANAGEMENT ------------------------------ ##
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
        assert handler_name not in self._handlers_map, f"Handler {handler_name} already exists. Please use update_handler to modify it."
        # Validate required keys first
        assert "level" in handler_conf, f"Handler {handler_name} must have a 'level' key. Please define a level for the handler in the config file."
        # Store handler's base level before modifying config
        handler_base_level = handler_conf["level"].upper()
        self._handlers_map[handler_name]["base_level"] = handler_base_level
        # modify handler config
        handler_conf = self._modify_handler_conf(handler_name, handler_conf, self.config.get("formats", {}))
        # add handler and update handlers map
        self._handlers_map[handler_name]["id"] = logger.add(**handler_conf)

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
        assert handler_name in self._handlers_map, f"Handler {handler_name} does not exist. Please use add_handler to create it."
        # Validate required keys first
        assert "level" in handler_conf, f"Handler {handler_name} must have a 'level' key. Please define a level for the handler in the config file."
        # get current handler info
        old_handler_id = self._handlers_map[handler_name]["id"]
        # remove old handler
        logger.remove(old_handler_id)
        # store new handler's base level before modifying config
        handler_base_level = handler_conf["level"].upper()
        self._handlers_map[handler_name]["base_level"] = handler_base_level
        # modify handler_config if needed
        handler_conf = self._modify_handler_conf(handler_name, handler_conf, self.config.get("formats", {}))
        # add new handler and get new handler id
        new_handler_id = logger.add(**handler_conf)
        # update handlers map with new handler id
        self._handlers_map[handler_name]["id"] = new_handler_id

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
        assert handler_name in self._handlers_map, f"Handler {handler_name} does not exist."
        # get current handler info
        old_handler_id = self._handlers_map[handler_name]["id"]
        # remove handler
        logger.remove(old_handler_id)
        loggers = self._handlers_map.pop(handler_name).get("loggers", {})
        self._remove_handler_mapping(handler_name, loggers)

    def _remove_handler_mapping(self, handler_name: str, loggers: dict[str, dict]):
        """
        Remove the handler mapping for the specified handler name.
        
        Args:
            handler_name (str): The name of the handler to remove.
            loggers (dict[str, dict]): A dictionary of loggers associated with the handler.
            
        This method cleans up the logger mappings by removing all references to
        the specified handler from each logger's handler list.
        """
        for logger_name in loggers:
            if logger_name in self._loggers_map:
                # remove all dicts with 'handler' == handler_name from logger's handlers
                self._loggers_map[logger_name] = [
                    h for h in self._loggers_map[logger_name] if h["handler"] != handler_name
                ]
    
    ## ------------------------------ HANDLER CONFIG HANDLING ------------------------------ ##

    def _modify_handler_conf(self, handler_name: str, handler_conf: dict, format_conf: dict):
        """
        Modify the handler configuration by applying format and other settings.

        This method performs the following operations:
        - Extracts the format string from the handler configuration and replaces it 
            with the actual format configuration
        - Converts the sink string (if it is "sys.stdout" or "sys.stderr") to the 
            actual stream object
        - Ensures that the level is in uppercase
        - Adds a filter to the handler based on the logger mappings

        Args:
            handler_name (str): The name of the handler to modify.
            handler_conf (dict): The current configuration of the handler.
            format_conf (dict): The available format configurations.

        Returns:
            dict: The modified handler configuration.

        Raises:
            AssertionError: If the 'sink' key is missing or invalid in the handler configuration.
            AssertionError: If the 'level' key is missing or invalid in the handler configuration.
            AssertionError: If the 'format' key is missing or invalid in the handler configuration.
        """
        assert "sink" in handler_conf, f"Handler {handler_name} must have a 'sink' key. Please define a sink for the handler in the config file."
        assert "level" in handler_conf, f"Handler {handler_name} must have a 'level' key. Please define a level for the handler in the config file."
        assert "format" in handler_conf, f"Handler {handler_name} must have a 'format' key. Please define a format for the handler in the config file."

        format_str = handler_conf["format"]

        extracted_format = format_conf.get(format_str, {})
        if not extracted_format:
            sys.stderr.write(
                f" ⚠️ The format referenced by handler '{handler_name}' is not defined in the 'formats' section of the config file."
                f" Using the format as is: \n"
                f"\t {format_str} \n\n"
            )
        else:
            handler_conf["format"] = extracted_format

        # convert sink string "sys.stdout" or "sys.stderr" to actual stream objects
        if handler_conf.get("sink") == "sys.stdout":
            handler_conf["sink"] = sys.stdout
        elif handler_conf.get("sink") == "sys.stderr":
            handler_conf["sink"] = sys.stderr
        
        # ensure level is in uppercase
        handler_conf["level"] = handler_conf["level"].upper()

        # add filter
        handler_conf["filter"] = self._make_handler_filter(handler_name)

        return handler_conf
    
    def _make_handler_filter(self, handler_name: str):
        """
        Create a filter function for the handler based on the logger mappings.

        The returned filter function implements the design where the effective threshold
        is the maximum of the handler's base level and the logger's level. This ensures
        that a handler's base level acts as a minimum threshold that cannot be bypassed.

        Design Logic:
        - Handler ERROR, Logger DEBUG → Threshold = max(ERROR, DEBUG) = ERROR
        - Handler DEBUG, Logger INFO → Threshold = max(DEBUG, INFO) = INFO
        - Handler INFO, Logger WARNING → Threshold = max(INFO, WARNING) = WARNING

        Args:
            handler_name (str): The name of the handler for which to create the filter.

        Returns:
            function: A filter function that takes a log record and returns True if 
                the record should be processed by the handler, otherwise returns False.
        """

        def filter_func(record):
            logger_name = record["extra"].get("logger_name")
            record_level = record["level"].no
            
            # Check if this logger is configured for this handler
            if logger_name not in self._handlers_map[handler_name].get("loggers", {}):
                return False
            
            # Get handler's base level and logger's specific level
            handler_base_level = logger.level(self._handlers_map[handler_name]["base_level"]).no
            logger_level = logger.level(self._handlers_map[handler_name]["loggers"][logger_name]["level"]).no
            
            # Effective threshold is the maximum of handler base level and logger level
            effective_threshold = max(handler_base_level, logger_level)
            
            return record_level >= effective_threshold
        
        return filter_func

    ## ------------------------------ LOGGER MANAGEMENT ------------------------------ ##
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
        assert logger_name in self._loggers_map, f"Logger {logger_name} does not exist. Please add it first."
        return logger.bind(logger_name=logger_name)

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
        assert logger_name not in self._loggers_map, f"Logger {logger_name} already exists. Please use update_logger to modify it."
        self._add_logger_mapping(logger_name, handlers)
        self._loggers_map.update({logger_name: handlers})

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
        assert logger_name in self._loggers_map, f"Logger {logger_name} does not exist. Please use add_logger to create it."
        self.remove_logger(logger_name)
        self.add_logger(logger_name, handlers)

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
        assert logger_name in self._loggers_map, f"Logger {logger_name} does not exist."
        handlers = self._loggers_map.pop(logger_name)
        self._remove_logger_mapping(logger_name, handlers)

    def _add_logger_mapping(self, logger_name: str, handlers: list[tuple[str, dict]]):
        """
        Add a logger mapping for the specified logger name and handlers.

        Args:
            logger_name (str): The name of the logger to add.
            handlers (list[tuple[str, dict]]): A list of tuples where each tuple contains 
                a handler name and its configuration.
                
        This method updates the handlers map to include the logger and its
        level configuration for each specified handler.
        """
        for _handler in handlers:
            handler_name = _handler["handler"]

            if handler_name not in self._handlers_map:
                raise KeyError(f"Handler '{handler_name}' does not exist. Please add the handler before referencing it in a logger.")
            handler_params = {"level": _handler["level"].upper()} 
            self._handlers_map[handler_name]["loggers"] = self._handlers_map[handler_name].get("loggers", {})
            self._handlers_map[handler_name]["loggers"][logger_name] = handler_params

    def _remove_logger_mapping(self, logger_name: str, handlers: list[tuple[str, dict]]):
        """
        Remove the logger mapping for the specified logger name and handlers.
        
        Args:
            logger_name (str): The name of the logger to remove.
            handlers (list[tuple[str, dict]]): A list of tuples where each tuple contains 
                a handler name and its configuration.
                
        This method cleans up the handlers map by removing the logger from
        each specified handler's logger list.
        """
        for _handler in handlers:
            handler_name = _handler["handler"]
            if handler_name in self._handlers_map:
                self._handlers_map[handler_name]["loggers"].pop(logger_name, None)

    ## ------------------------------ HDFS COPY MANAGEMENT ------------------------------ ##
    def start_hdfs_copy(
        self,
        copy_name: str,
        path_patterns: List[str],
        hdfs_destination: str,
        root_dir: Optional[str] = None,
        copy_interval: int = 60,
        create_dest_dirs: bool = True,
        preserve_structure: bool = False,
        max_retries: int = 3,
        retry_delay: int = 5
    ) -> None:
        """
        Start a background thread to periodically copy log files from local to HDFS.
        
        Args:
            copy_name (str): Unique name for this copy operation (used for thread identification).
            path_patterns (List[str]): Glob pattern(s) or file path(s) to match local files.
                Examples: 
                - ["/path/to/logs/*.log"]
                - ["/path/to/logs/*.log", "/path/to/logs/*.txt"]
                - ["/path/to/specific/file.log"]
            hdfs_destination (str): HDFS destination directory path.
                Example: "hdfs://namenode:port/path/to/hdfs/logs/"
            root_dir (Optional[str]): Root directory for the local files.
            copy_interval (int): Interval in seconds between copy operations. Default is 60 seconds.
            create_dest_dirs (bool): Whether to create destination directories if they don't exist.
            preserve_structure (bool): Whether to preserve local directory structure in HDFS.
                If True, root_dir must be specified.
                If True: "/local/logs/app/file.log" -> "hdfs://dest/app/file.log"
                If False: "/local/logs/app/file.log" -> "hdfs://dest/file.log"
            max_retries (int): Maximum number of retry attempts for failed copies. Default is 3.
            retry_delay (int): Delay in seconds between retry attempts. Default is 5.
            
        Raises:
            ValueError: If copy_name already exists or parameters are invalid.
            
        Example:
            # Copy all .log files every 2 minutes
            log_manager.start_hdfs_copy(
                copy_name="app_logs",
                path_patterns="/tmp/logs/*.log",
                hdfs_destination="hdfs://namenode:9000/logs/app/",
                copy_interval=120
            )
            
            # Manually trigger additional copy if needed
            log_manager.trigger_hdfs_copy_now("app_logs")
            
        Copy Behavior:
            - Periodic copy: Files are copied every copy_interval seconds  
            - Final copy: Files are copied one last time during cleanup/shutdown
            - Manual copy: Files can be copied on-demand using trigger_hdfs_copy_now()
            
        Potential Issues & Concerns:
            1. Network connectivity: HDFS copy operations may fail due to network issues.
               The function includes retry logic to handle temporary failures.
            
            2. File locking: If a log file is being written while copying, it may cause issues.
               Consider using log rotation to avoid conflicts.
            
            3. Storage space: Monitor HDFS space to prevent copy failures due to insufficient space.
            
            4. Performance: Frequent copying of large files may impact system performance.
               Adjust copy_interval based on file sizes and system capacity.
            
            5. Permissions: Ensure proper read permissions on local files and write permissions on HDFS.
            
            6. Thread safety: Multiple copy operations run in separate threads.
               Each operation is independent and won't interfere with Python logging.
        """

        # Validate parameters
        if not copy_name:
            raise ValueError("copy_name cannot be empty")
        if self._shutdown_in_progress:
            raise ValueError("Cannot start new HDFS copy operations: LogManager is shutting down")
        if copy_name in self._hdfs_copy_threads:
            raise ValueError(f"HDFS copy operation '{copy_name}' already exists. Use stop_hdfs_copy() first.")
        if not path_patterns:
            raise ValueError("path_patterns cannot be empty")
        if not hdfs_destination:
            raise ValueError("hdfs_destination cannot be empty")
        if copy_interval <= 0:
            raise ValueError("copy_interval must be positive")
        if max_retries < 0:
            raise ValueError("max_retries cannot be negative")
        if retry_delay < 0:
            raise ValueError("retry_delay cannot be negative")
        if preserve_structure and not root_dir:
            raise ValueError(
                "'root_dir' must be specified when 'preserve_structure' is True. "
                "This is required to maintain the directory structure in HDFS."
            )
        
        # Create stop event for this copy operation
        stop_event = threading.Event()
        self._stop_events[copy_name] = stop_event
        
        # Create and start the copy thread
        copy_thread = threading.Thread(
            target=self._hdfs_copy_worker,
            args=(
                copy_name,
                path_patterns,
                hdfs_destination,
                copy_interval,
                create_dest_dirs,
                preserve_structure,
                root_dir,
                max_retries,
                retry_delay,
                stop_event
            ),
            daemon=False,
            name=f"HDFSCopy-{copy_name}"
        )
        
        self._hdfs_copy_threads[copy_name] = copy_thread
        self._copy_operations_files[copy_name] = set()  # Initialize empty file set
        
        # Store copy parameters for later use (e.g., manual triggers, final copy)
        self._copy_operations_params[copy_name] = {
            'path_patterns': path_patterns,
            'hdfs_destination': hdfs_destination,
            'create_dest_dirs': create_dest_dirs,
            'preserve_structure': preserve_structure,
            'root_dir': root_dir,
            'max_retries': max_retries,
            'retry_delay': retry_delay
        }
        
        copy_thread.start()
        
        print(f"Started HDFS copy operation '{copy_name}' with {copy_interval}s interval.\n")

    def stop_hdfs_copy(self, copy_name: str, timeout: float = 10.0) -> bool:
        """
        Stop a running HDFS copy operation.
        
        Args:
            copy_name (str): Name of the copy operation to stop.
            timeout (float): Maximum time to wait for thread to stop. Default is 10 seconds.
            
        Returns:
            bool: True if successfully stopped, False if timeout occurred.
            
        Raises:
            ValueError: If copy_name doesn't exist.
        """
        if copy_name not in self._hdfs_copy_threads:
            raise ValueError(f"HDFS copy operation '{copy_name}' does not exist")
        
        # Signal the thread to stop
        self._stop_events[copy_name].set()
        
        # Wait for thread to finish
        self._hdfs_copy_threads[copy_name].join(timeout=timeout)
        
        # Check if thread actually stopped
        if self._hdfs_copy_threads[copy_name].is_alive():
            print(
                f"Warning: HDFS copy thread '{copy_name}' did not stop within {timeout}s.\n"
                f"Consider increasing the timeout or checking the thread manually."
            )
            return False
        
        del self._hdfs_copy_threads[copy_name]
        del self._stop_events[copy_name]
        del self._copy_operations_files[copy_name]  # Clean up file tracking
        del self._copy_operations_params[copy_name]  # Clean up parameter storage
        
        print(f"Stopped HDFS copy operation '{copy_name}'")
        return True

    def stop_all_hdfs_copy(self, timeout: float = 30.0, verbose: bool = False) -> List[str]:
        """
        Stop all running HDFS copy operations.
        
        Args:
            timeout (float): Maximum time to wait for each thread to stop. Defaults to 30 seconds.
            verbose (bool): Whether to provide detailed feedback during the operation. Defaults to False.
            
        Returns:
            List[str]: Names of copy operations that failed to stop within timeout.
        """
        if not self._hdfs_copy_threads:
            return []
            
        if verbose:
            print(f"Stopping {len(self._hdfs_copy_threads)} HDFS copy operation(s)...")
            
        failed_to_stop = []
        copy_names = list(self._hdfs_copy_threads.keys())
        
        for copy_name in copy_names:
            try:
                if not self.stop_hdfs_copy(copy_name, timeout):
                    failed_to_stop.append(copy_name)
            except ValueError as e:
                print(f"Error stopping HDFS copy operation '{copy_name}': {e}")
                failed_to_stop.append(copy_name)
        
        if verbose:
            if failed_to_stop:
                print(
                    f"WARNING: Some HDFS copy operations did not stop cleanly: {failed_to_stop}\n"
                    f"Please check the threads manually."
                )
            else:
                print("All HDFS copy operations stopped successfully.")
                
        return failed_to_stop

    def list_hdfs_copy_operations(self) -> List[dict]:
        """
        List all active HDFS copy operations.
        
        Returns:
            List[dict]: Information about active copy operations.
        """
        operations = []
        for copy_name, thread in self._hdfs_copy_threads.items():
            operations.append({
                "name": copy_name,
                "thread_name": thread.name,
                "is_alive": thread.is_alive(),
                "daemon": thread.daemon
            })
        return operations
    
    def trigger_hdfs_copy_now(self, copy_name: Optional[str] = None) -> None:
        """
        Manually trigger an immediate copy operation for specific or all HDFS operations.
        
        This method allows you to force a copy operation outside of the normal
        interval schedule. Useful for ensuring log files are copied before
        critical operations or during testing.
        
        Args:
            copy_name (Optional[str]): Name of specific copy operation to trigger.
                                     If None, triggers all active operations.
                                     
        Raises:
            ValueError: If copy_name is specified but doesn't exist.
            
        Example:
            # Trigger specific operation
            log_manager.trigger_hdfs_copy_now("my_copy_operation")
            
            # Trigger all operations  
            log_manager.trigger_hdfs_copy_now()
        """
        if not self._hdfs_copy_threads:
            print("No active HDFS copy operations to trigger.")
            return
            
        if copy_name is not None:
            if copy_name not in self._hdfs_copy_threads:
                raise ValueError(f"HDFS copy operation '{copy_name}' does not exist")
            copy_names = [copy_name]
        else:
            copy_names = list(self._hdfs_copy_threads.keys())
            
        print(f"Manually triggering {len(copy_names)} HDFS copy operation(s)...")
        
        for name in copy_names:
            try:
                # Use stored parameters instead of extracting from thread args
                params = self._copy_operations_params[name]
                self._perform_copy_operation(
                    name,
                    params['path_patterns'],
                    params['hdfs_destination'],
                    params['create_dest_dirs'],
                    params['preserve_structure'],
                    params['root_dir'],
                    params['max_retries'],
                    params['retry_delay']
                )
                    
            except Exception as e:
                print(f"Exception occured during manually-triggered copy operation for '{name}': {e}")

    def _hdfs_copy_worker(
        self,
        copy_name: str,
        path_patterns: List[str],
        hdfs_destination: str,
        copy_interval: int,
        create_dest_dirs: bool,
        preserve_structure: bool,
        root_dir: Optional[str],
        max_retries: int,
        retry_delay: int,
        stop_event: threading.Event
    ) -> None:
        """
        Worker function that runs in a separate thread to perform periodic HDFS copying.
        """
        print(f"HDFS copy worker '{copy_name}' started.")

        while not stop_event.is_set():
            # Perform periodic copy
            self._perform_copy_operation(
                copy_name, 
                path_patterns,
                hdfs_destination,
                create_dest_dirs,
                preserve_structure,
                root_dir,
                max_retries,
                retry_delay
            )

            # Wait for the interval
            if stop_event.wait(timeout=copy_interval):
                break

        print(f"HDFS copy worker '{copy_name}' stopped")
        
    def _perform_copy_operation(
        self,
        copy_name: str,
        path_patterns: List[str],
        hdfs_destination: str,
        create_dest_dirs: bool,
        preserve_structure: bool,
        root_dir: Optional[str],
        max_retries: int,
        retry_delay: int
    ) -> None:
        """
        Perform a single copy operation.
        
        Args:
            copy_name (str): Name of the copy operation for logging.
            path_patterns (List[str]): File patterns to search for.
            hdfs_destination (str): HDFS destination path.
            create_dest_dirs (bool): Whether to create destination directories.
            preserve_structure (bool): Whether to preserve directory structure.
            root_dir (Optional[str]): Root directory for relative paths.
            max_retries (int): Maximum number of retry attempts.
            retry_delay (int): Delay between retries in seconds.
        """
        try:
            # Find files matching the provided patterns
            files_to_copy = self._discover_files_to_copy(path_patterns)
            
            if files_to_copy:
                print(f"HDFS copy '{copy_name}' found {len(files_to_copy)} files to copy.")
                
                # Check for duplicate files across operations and issue warnings
                self._check_for_duplicate_files(copy_name, files_to_copy)
                
                self._copy_files_to_hdfs(
                    files_to_copy,
                    hdfs_destination,
                    create_dest_dirs,
                    preserve_structure,
                    root_dir,
                    max_retries,
                    retry_delay
                )
            else:
                print(
                    f"HDFS copy '{copy_name}': No files found matching patterns {path_patterns}. "
                    f"No files copied in this cycle."
                )
                
        except Exception as e:
            print(f"Error in HDFS copy operation '{copy_name}': {e}")

    def _discover_files_to_copy(self, path_patterns: List[str]) -> List[str]:
        """
        Discover files matching the provided patterns.
        
        Args:
            path_patterns (List[str]): List of file paths or glob patterns.
            
        Returns:
            List[str]: List of unique file paths that exist.
        """
        files_to_copy = []
        
        for pattern in path_patterns:
            if os.path.isfile(pattern):
                files_to_copy.append(pattern)
            else:
                try:
                    matched_files = glob.glob(pattern, recursive=True)
                    files_to_copy.extend([f for f in matched_files if os.path.isfile(f)])
                except (OSError, ValueError) as e:
                    print(f"Warning: Invalid pattern '{pattern}': {e}")
        
        return list(set(files_to_copy))

    def _check_for_duplicate_files(self, copy_name: str, files_to_copy: List[str]) -> None:
        """
        Check if any files in files_to_copy are already being copied by other operations.
        Issues warnings for duplicate files but doesn't prevent copying.
        
        Args:
            copy_name (str): Name of the current copy operation.
            files_to_copy (List[str]): List of files that this operation wants to copy.
        """
        if not files_to_copy:
            return
            
        current_files = set(files_to_copy)
        
        # Check against all other active operations
        for other_copy_name, other_files in self._copy_operations_files.items():
            if other_copy_name == copy_name:
                continue
                
            overlapping_files = current_files.intersection(other_files)
            if overlapping_files:
                print(
                    f"WARNING: copy operation '{copy_name}' and '{other_copy_name}' "
                    f"are both copying {len(overlapping_files)} file(s):"
                )
                for file_path in sorted(overlapping_files):
                    print(f"  - {file_path}")
                print(
                    f"This may cause race conditions or unnecessary resource usage. "
                    f"Consider adjusting your copy operation patterns to avoid overlaps.\n"
                )

        # Update the file set for this operation
        self._copy_operations_files[copy_name] = current_files

    def _copy_files_to_hdfs(
        self,
        local_files: List[str],
        hdfs_destination: str,
        create_dest_dirs: bool,
        preserve_structure: bool,
        root_dir: Optional[str],
        max_retries: int,
        retry_delay: int
    ) -> None:
        """
        Copy a list of local files to HDFS destination.
        """
        success_count = 0
        error_count = 0
        
        for local_file in local_files:
            if preserve_structure:
                rel_path = os.path.relpath(local_file, root_dir)
                dest_path = os.path.join(hdfs_destination, rel_path).replace("\\", "/")
            else:
                filename = os.path.basename(local_file)
                dest_path = os.path.join(hdfs_destination, filename).replace("\\", "/")
            
            if create_dest_dirs:
                dest_dir = os.path.dirname(dest_path)
                try:
                    FileIOInterface.fmakedirs(dest_dir, exist_ok=True)
                except Exception as e:
                    print(f"Warning: Could not create directory {dest_dir}: {e}")

            for attempt in range(max_retries + 1):
                try:
                    # Copy file using FileIO interface
                    FileIOInterface.fcopy(
                        read_path=local_file,
                        dest_path=dest_path,
                    )
                    
                    success_count += 1
                    print(f"Successfully copied {local_file} -> {dest_path}")
                    break
                    
                except Exception as e:
                    if attempt < max_retries:
                        print(f"Attempt {attempt + 1} failed for {local_file}: {e}. Retrying in {retry_delay}s...")
                        time.sleep(retry_delay)
                    else:
                        print(f"Failed to copy {local_file} after {max_retries + 1} attempts: {e}")
                        error_count += 1
        
        if success_count > 0 or error_count > 0:
            print(f"HDFS copy completed: {success_count} successful, {error_count} failed")

    ## ------------------------------ TEARDOWN ------------------------------ ##
    def _cleanup(self, hdfs_timeout: float = 60.0):
        """
        Cleanup function to remove all handlers and loggers.
        
        This method is automatically called on program exit via atexit.register().
        It ensures proper cleanup of all Loguru handlers and clears the internal
        mapping dictionaries. It also performs a final copy operation for all
        active HDFS operations before stopping them.
        
        Args:
            hdfs_timeout (float): Timeout in seconds for stopping HDFS operations.
                                    Defaults to 60.0.
        
        Note: This method can be called multiple times safely.
        """
        if self._shutdown_in_progress:
            return
        
        self._shutdown_in_progress = True
        
        print("LogManager cleanup initiated...")
        
        # Perform final copy operation before stopping threads
        if self._hdfs_copy_threads:
            print("Performing final HDFS copy before shutdown...")
            try:
                self.trigger_hdfs_copy_now()
            except Exception as e:
                print(f"Warning: Final HDFS copy failed during cleanup: {e}")
        
        self.stop_all_hdfs_copy(timeout=hdfs_timeout, verbose=True)
        logger.remove()
        self._handlers_map.clear()
        self._loggers_map.clear()
        self._copy_operations_files.clear()
        self._copy_operations_params.clear()
        print("LogManager cleanup completed.")