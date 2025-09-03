"""
LoggingManager - Handles Loguru configuration and logger management.

This module provides centralized logging configuration and handler management
built on top of Loguru.
"""

import os
import sys
import yaml
from pathlib import Path
from typing import Optional, Dict, List, Tuple
from collections import defaultdict

from loguru import logger


class LoggingManager:
    """
    LoggingManager class to manage logging configuration and handlers.
    
    This class provides a centralized way to manage Loguru loggers and handlers
    using YAML configuration files. It supports dynamic handler and logger
    management with automatic cleanup.
    """
    
    DEFAULT_CONFIG_PATH = Path(__file__).parent / "_default_logger_config.yaml"
    
    def __init__(
        self,
        config_path: Optional[str] = None,
        timezone: str = "Asia/Singapore",
    ):
        """
        Initialize LoggingManager with a configuration file path and timezone.
        
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
        logger.remove()                     # remove default logger
        self._config_path = config_path     # empty config_path is handled in _setup_logger
        self.config = {}
        self._setup_logger()
    
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
        if "sink" not in handler_conf:
            raise AssertionError(f"Handler {handler_name} must have a 'sink' key. Please define a sink for the handler in the config file.")
        if "level" not in handler_conf:
            raise AssertionError(f"Handler {handler_name} must have a 'level' key. Please define a level for the handler in the config file.")
        if "format" not in handler_conf:
            raise AssertionError(f"Handler {handler_name} must have a 'format' key. Please define a format for the handler in the config file.")

        format_str = handler_conf["format"]

        extracted_format = format_conf.get(format_str, {})
        if not extracted_format:
            sys.stdout.write(
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

    ## ------------------------------ CLEANUP ------------------------------ ##
    def cleanup(self):
        """
        Cleanup function to remove all handlers and loggers.
        
        This method ensures proper cleanup of all Loguru handlers and clears the internal
        mapping dictionaries.
        
        Note: This method can be called multiple times safely.
        """
        print("Logger cleanup initiated...")
        logger.remove()
        self._handlers_map.clear()
        self._loggers_map.clear()
        print("Logger cleanup completed.")
