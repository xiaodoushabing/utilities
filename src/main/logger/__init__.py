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
        # if platform.system() == "Windows":
        #     # Windows requires tzset to apply the timezone change
        #     import time as win_time
        #     win_time.tzset()
        # else:
        #     # For Unix-like systems, use the standard time module
        #     time.tzset()

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
        self._config_path = config_path         # Empty config_path is handled in _setup_logger
        self.config = {}
        self._setup_logger()

        # teardown
        atexit.register(self._cleanup)
        
        # HDFS copy management
        self._hdfs_copy_threads = {}  # thread_name -> thread object
        self._stop_events = {}        # thread_name -> threading.Event

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
                self._handlers_map[handler_name]["loggers"].pop(logger_name)

    ## ------------------------------ HDFS COPY MANAGEMENT ------------------------------ ##
    def start_hdfs_copy(
        self,
        copy_name: str,
        local_pattern: Union[str, List[str]],
        hdfs_destination: str,
        copy_interval: int = 60,
        filesystem: Optional[str] = "hdfs",
        create_dest_dirs: bool = True,
        preserve_structure: bool = True,
        max_retries: int = 3,
        retry_delay: int = 5
    ) -> None:
        """
        Start a background thread to periodically copy log files from local to HDFS.
        
        Args:
            copy_name (str): Unique name for this copy operation (used for thread identification).
            local_pattern (Union[str, List[str]]): Glob pattern(s) or file path(s) to match local files.
                Examples: 
                - "/path/to/logs/*.log"
                - ["/path/to/logs/*.log", "/path/to/logs/*.txt"]
                - "/path/to/specific/file.log"
            hdfs_destination (str): HDFS destination directory path.
                Example: "hdfs://namenode:port/path/to/hdfs/logs/"
            copy_interval (int): Interval in seconds between copy operations. Default is 60 seconds.
            filesystem (Optional[str]): Filesystem type for HDFS. Default is "hdfs".
            create_dest_dirs (bool): Whether to create destination directories if they don't exist.
            preserve_structure (bool): Whether to preserve local directory structure in HDFS.
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
                local_pattern="/tmp/logs/*.log",
                hdfs_destination="hdfs://namenode:9000/logs/app/",
                copy_interval=120
            )
            
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
            print(f"Warning: HDFS copy thread '{copy_name}' did not stop within {timeout}s")
            return False
        
        # Clean up
        del self._hdfs_copy_threads[copy_name]
        del self._stop_events[copy_name]
        
        print(f"Stopped HDFS copy operation '{copy_name}'")
        return True

    def stop_all_hdfs_copy(self, timeout: float = 10.0) -> List[str]:
        """
        Stop all running HDFS copy operations.
        
        Args:
            timeout (float): Maximum time to wait for each thread to stop.
            
        Returns:
            List[str]: Names of copy operations that failed to stop within timeout.
        """
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

    def _hdfs_copy_worker(
        self,
        copy_name: str,
        patterns: List[str],
        hdfs_destination: str,
        copy_interval: int,
        filesystem: str,
        create_dest_dirs: bool,
        preserve_structure: bool,
        max_retries: int,
        retry_delay: int,
        stop_event: threading.Event
    ) -> None:
        """
        Worker function that runs in a separate thread to perform periodic HDFS copying.
        
        This is an internal method that should not be called directly.
        """
        print(f"HDFS copy worker '{copy_name}' started")
        
        while not stop_event.is_set():
            try:
                # Find files matching patterns
                files_to_copy = []
                for pattern in patterns:
                    if os.path.isfile(pattern):
                        # Direct file path
                        files_to_copy.append(pattern)
                    else:
                        # Glob pattern
                        matched_files = glob.glob(pattern, recursive=True)
                        files_to_copy.extend(matched_files)
                
                # Remove duplicates and filter only files
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
        local_files: List[str],
        hdfs_destination: str,
        filesystem: str,
        create_dest_dirs: bool,
        preserve_structure: bool,
        max_retries: int,
        retry_delay: int
    ) -> None:
        """
        Copy a list of local files to HDFS destination.
        
        This is an internal method that handles the actual file copying logic.
        """
        success_count = 0
        error_count = 0
        
        for local_file in local_files:
            for attempt in range(max_retries + 1):
                try:
                    # Determine destination path
                    if preserve_structure:
                        # Preserve directory structure
                        rel_path = os.path.basename(local_file)
                        dest_path = os.path.join(hdfs_destination, rel_path).replace("\\", "/")
                    else:
                        # Flatten structure
                        filename = os.path.basename(local_file)
                        dest_path = os.path.join(hdfs_destination, filename).replace("\\", "/")
                    
                    # Create destination directory if needed
                    if create_dest_dirs:
                        dest_dir = os.path.dirname(dest_path)
                        try:
                            FileIOInterface.fmakedirs(dest_dir, filesystem=filesystem, exist_ok=True)
                        except Exception as mkdir_error:
                            print(f"Warning: Could not create directory {dest_dir}: {mkdir_error}")
                    
                    # Copy file using FileIO interface
                    FileIOInterface.fcopy(
                        read_path=local_file,
                        dest_path=dest_path,
                        filesystem=filesystem
                    )
                    
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

    ## ------------------------------ TEARDOWN ------------------------------ ##
    def _cleanup(self):
        """
        Cleanup function to remove all handlers and loggers.
        
        This method is automatically called on program exit via atexit.register().
        It ensures proper cleanup of all Loguru handlers and clears the internal
        mapping dictionaries. It also stops all HDFS copy operations.
        """
        # Stop all HDFS copy operations
        failed_to_stop = self.stop_all_hdfs_copy(timeout=5.0)
        if failed_to_stop:
            print(f"Warning: Some HDFS copy operations did not stop cleanly: {failed_to_stop}")
        
        logger.remove()
        self._handlers_map.clear()
        self._loggers_map.clear()