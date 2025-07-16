import atexit
import inspect
import os
import yaml
import sys
from pathlib import Path
from typing import Optional
from collections import defaultdict
import pendulum
from loguru import logger

class LogManager:
    """
    A comprehensive logging manager that provides task-based logging with configurable handlers.
    
    This class manages loguru loggers with support for multiple handlers, task-based filtering,
    and YAML configuration files.
    
    Attributes:
        TZ_UTC8 (pendulum.Timezone): Default timezone for log timestamps.
        DEFAULT_LOG_DIR (Path): Default directory for log files.
        DEFAULT_TASK (str): Default task name for logging operations.
    """

    # Define default variables
    TZ_UTC8 = pendulum.timezone("Asia/Singapore")
    DEFAULT_LOG_DIR = Path(os.getcwd()) / ".logs"    # Assume run starts from the project root
    DEFAULT_TASK = "main"

    def __init__(
            self,
            log_dir: Optional[str] = "",
            name: Optional[str] = "",
            task: Optional[str] = "",
            config_file: Optional[str] = "",
    ):
        """
        Initialize the LogManager with configuration options.
        
        Args:
            log_dir (Optional[str]): Directory path for log files. Defaults to ".logs" in current directory.
            name (Optional[str]): Logger name. Auto-detected from calling module if not provided.
            task (Optional[str]): Default task name for logging operations. Defaults to "main".
            config_file (Optional[str]): Path to YAML configuration file. Uses default settings if not provided.
            
        Note:
            The logger is automatically configured and cleanup is registered for program exit.
        """
        
        # Get log directory
        self.log_dir = Path(log_dir) if log_dir else self.DEFAULT_LOG_DIR

        # Logger metadata
        self.name = name if name else self._get_caller_name()
        self.task = task if task else self.DEFAULT_TASK

        # Logger mappers
        self.logger_handlers_map = defaultdict(dict)    # handler_name -< dict of {task: level}
        self.logger_tasks_map = defaultdict(dict)       # task -> dict of {handler_name: level}
        self.handler_ids = {}                           # handler_name -> handler_id

        # Setup logger
        logger.remove()  # Remove default logger
        self.config_file = config_file
        self._setup_logger(self.config_file)

        # Teardown logger on exit
        atexit.register(self._teardown_logger)

    ########################################### SETUP LOGGER ###########################################
    def _setup_logger(self, config_path: Optional[str] = None):
        """
        Setup logger with configuration from YAML file or default settings.

        This method initializes the logger either from a YAML configuration file or
        with default settings if no config file is provided or if the file is invalid.

        Args:
            config_path (Optional[str]): Path to the YAML configuration file.
            
        Note:
            Falls back to default configuration if config file is missing or invalid.
        """
        # If no configuration path is provided, add default handler
        if not config_path or not Path(config_path).is_file():
            if config_path:
                print(f"Configuration file {config_path} not found. Using default settings.")
            print("Initializing logger with default settings.")
            self._default_logger_setup()
            logger.info("Logger initialized with default values", logger_task=self.task, name=self.name)
            return

        # Else, load configuration from YAML file
        try:
            self._setup_from_yaml(config_path)
            logger.info("Logger initialized from configuration file", logger_task=self.task, name=self.name)
        except Exception as e:
            logger.error(f"Failed to initialize logger from configuration file: {e}")
            print(f"Error loading configuration file: {e}")
            self._default_logger_setup()
    
    def _default_logger_setup(self):
        """
        Setup default logger with class defaults.
        
        Creates console and file handlers with default formatting and filtering.
        Used as fallback when no configuration file is provided or when config loading fails.
        
        Note:
            Both handlers are configured with DEBUG level and task-based filtering.
        """
        
        # Configure default logger
        logger.configure(
            patcher=self.convert_log_time,
            extra={
                "name": self.name,
                "logger_task": self.task,
            }
        )

        # Add default handlers
        handlers = {
            # Note: A new sink requires a unique handler name
            "console": {
                "sink": sys.stdout,
                "format": "<green>{time:YYYY-MM-DD HH:mm:ss.SSS}</green> | <level>{level: <8}</level> | "
                            "<cyan>{extra[name]}</cyan> | <magenta>{extra[logger_task]}</magenta> | <cyan>{file: <16} |"
                            "{function}:{line}</cyan> | <level>{message}</level>",
                "level": "DEBUG",
                "colorize": True,
                "enqueue": False,
                "filter": self._make_handler_filter("console"),
            },
            "file": {
                "sink": self.log_dir / f"{self.name}.log",
                "format": "<green>{time:YYYY-MM-DD HH:mm:ss.SSS}</green> | <level>{level: <8}</level> | "
                            "<cyan>{extra[name]}</cyan> | <magenta>{extra[logger_task]}</magenta> | <cyan>{file: <16} |"
                            "{function}:{line}</cyan> | <level>{message}</level>",
                "level": "DEBUG",
                "enqueue": False,
                "colorize": True,
                "filter": self._make_handler_filter("file"),
            }
        }

        # Add handlers to logger
        self.setup_handlers(handlers)
        # Update logger tasks and handlers maps
        self.add_task(task=self.task, handlers=[("console", "DEBUG"), ("file", "DEBUG")])

    def _setup_from_yaml(self, config_path: str):
        """
        Setup logger from YAML configuration file.

        Loads and parses a YAML configuration file to set up handlers, formats, and tasks.
        Supports custom log formats, multiple handlers, and task-based logging configurations.

        Args:
            config_path (str): Path to the YAML configuration file.
            
        Note:
            The YAML file should contain 'handlers', 'formats', and 'logger_tasks' sections.
        """
        
        # Configure logger
        logger.configure(
            patcher=self.convert_log_time,
            extra={
                "name": self.name,
            }
        )

        # Load YAML configuration
        with open(config_path, 'r') as file:
            config = yaml.safe_load(file)

        # Load handlers from configuration
        handlers = config.get("handlers", {})
        formats = config.get("formats", {})
        for handler_name, handler_config in handlers.items():
            # Convert sink string "sys.stdout" and "sys.stderr" to actual objects
            if handler_config.get("sink") == "sys.stdout":
                handlers[handler_name]["sink"] = sys.stdout
            elif handler_config.get("sink") == "sys.stderr":
                handlers[handler_name]["sink"] = sys.stderr
            # Ensure level is uppercase
            if "level" in handler_config:
                handlers[handler_name]["level"] = handler_config["level"].upper()
            # Get format for the handler
            format_str = handler_config.get("format")
            format = formats[format_str]
            handlers[handler_name]["format"] = format
            # Add filter to the handler
            handlers[handler_name]["filter"] = self._make_handler_filter(handler_name)
        self.setup_handlers(handlers)

        # Load tasks from configuration
        logger_tasks = config.get("logger_tasks", {})
        for task, handlers in logger_tasks.items():
            filtered_handlers = []
            for h in handlers:
                # Add handler name and level to the filtered handlers list. Also ensure level is uppercase
                filtered_handlers.append((h.get("handler"), h.get("level")))
            # Update logger tasks and handlers maps
            self.add_task(task=task, handlers=filtered_handlers)

    ############################################ HANDLER MANAGEMENT ###########################################
    def setup_handlers(self, handlers: dict):
        """
        Setup handlers for the logger.

        Adds multiple handlers to the logger with their respective configurations.
        Each handler is registered with a unique name and ID for later management.

        Args:
            handlers (dict): Dictionary of handlers to be added. Each key is the handler name
                           and value is a dictionary containing handler configuration (sink, format, level, etc.).
                           
        Raises:
            ValueError: If a handler name already exists.
            
        Note:
            Clears existing handler IDs before adding new handlers.
        """
        self.handler_ids.clear()
        for handler_name, handler_config in handlers.items():
            # Ensure handler name is unique
            if handler_name in self.handler_ids:
                raise ValueError(f"Handler name '{handler_name}' already exists.")
            # Add handler to logger
            self.handler_ids[handler_name] = logger.add(**handler_config)

    def remove_handler_by_name(self, handler_name: str):
        """
        Remove a handler by its name.

        Removes the specified handler from the logger and cleans up all associated
        mappings in tasks and handlers maps.

        Args:
            handler_name (str): Name of the handler to be removed.
            
        Raises:
            ValueError: If the handler name doesn't exist.
            
        Note:
            This operation will affect all tasks that were using this handler.
        """
        if handler_name in self.handler_ids:
            handler_id = self.handler_ids[handler_name]
            # Remove handler from logger
            logger.remove(handler_id)
            del self.handler_ids[handler_name]
            # Remove from logger tasks map
            for task in self.logger_tasks_map:
                if handler_name in self.logger_tasks_map[task]:
                    del self.logger_tasks_map[task][handler_name]
            # Remove from logger handlers map
            if handler_name in self.logger_handlers_map:
                del self.logger_handlers_map[handler_name]
        else:
            raise ValueError(f"Handler '{handler_name}' does not exist.")
    
    ############################################# TASK MANAGEMENT ###########################################
    def add_task(self, task: str, handlers: list[tuple[str, str]]):
        """
        Add a task with its associated handlers and log levels.

        Creates or updates a logging task with specific handlers and their minimum log levels.
        Tasks allow different parts of an application to have different logging configurations.

        Args:
            task (str): Name of the task (e.g., "main", "background", "api").
            handlers (list[tuple[str, str]]): List of tuples containing handler name and minimum log level.
                                            Example: [("console", "DEBUG"), ("file", "INFO")]
                                            
        Raises:
            ValueError: If the handlers list is empty.
            
        Note:
            If a task already exists, its handler configuration will be updated.
            Handler levels should be valid loguru levels: DEBUG, INFO, WARNING, ERROR, CRITICAL.
        """
        if not handlers:
            raise ValueError("Handlers list cannot be empty.")
        for handler_name, level in handlers:
            # Update logger tasks map
            self.logger_tasks_map[task][handler_name] = level
            # Update logger handlers map
            self.logger_handlers_map[handler_name][task] = level

    def _make_handler_filter(self, handler_name: str):
        """
        Create a filter function for the handler based on task and level configuration.

        Generates a filter function that determines whether a log record should be processed
        by a specific handler based on the record's task and log level.

        Args:
            handler_name (str): Name of the handler to create the filter for.

        Returns:
            function: Filter function that takes a log record and returns True if the record
                     should be processed by this handler, False otherwise.
                     
        Note:
            The filter uses the logger_task from the log record's extra data and compares
            the log level against the configured minimum level for the task-handler combination.
        """
        def filter_func(record):
            task = record["extra"].get("logger_task")
            level = record["level"].no
            if handler_name in self.logger_handlers_map:
                if task in self.logger_handlers_map[handler_name]:
                    return level >= logger.level(self.logger_handlers_map[handler_name][task]).no
            return False
        return filter_func
    
    ############################################## UTILITY METHODS ###########################################
    def _get_caller_name(self) -> str:
        """
        Get the name of the calling module for automatic logger naming.

        Inspects the call stack to determine the filename of the module that
        instantiated the LogManager, providing automatic logger naming.

        Returns:
            str: Name of the calling module (filename without extension) or "unknown" if detection fails.
        """
        frame = inspect.stack()[2]
        # module = inspect.getmodule(frame[0])
        # if module:
        #     return module.__name__
        filename = Path(frame.filename).stem
        return filename if filename else "unknown"

    def convert_log_time(self, record: str):
        """
        Convert log timestamps to the configured timezone.

        Patches log records to convert timestamps from UTC to the configured timezone (Asia/Singapore).
        This function is used as a patcher in loguru's configure method.

        Args:
            record (dict): The log record containing timestamp and other log data.

        Note:
            Modifies the record in-place, converting the 'time' field to TZ_UTC8 timezone.
            Used internally by loguru's configuration system.
        """
        record["time"] = pendulum.instance(record["time"]).in_tz(self.TZ_UTC8)

    def _teardown_logger(self):
        """
        Clean up logger resources and remove all handlers on program exit.
        
        This method is automatically called when the program exits (registered with atexit).
        It ensures proper cleanup of all logger handlers and internal data structures.
        
        Note:
            Removes all loguru handlers, clears internal mappings, and prints cleanup confirmation.
        """
        print("Exiting LogManager and cleaning up logger.")
        # Remove all handlers
        logger.remove()
        self.handler_ids.clear()
        self.logger_handlers_map.clear()
        self.logger_tasks_map.clear()
        print("Logger has been cleaned up and all handlers removed.")

    ############################################## MAPPINGS ###########################################
    def get_mappings(self, handlers = True, tasks = True):
        """
        Get the current mappings of handlers and tasks for debugging and inspection.

        Provides access to the internal mapping structures that track relationships
        between handlers, tasks, and log levels.

        Args:
            handlers (bool): Whether to include handler mappings (handler -> {task: level}).
            tasks (bool): Whether to include task mappings (task -> {handler: level}).

        Returns:
            tuple or dict: 
                - If both handlers and tasks are True: (handlers_map, tasks_map)
                - If only handlers is True: handlers_map
                - If only tasks is True: tasks_map
                
        Note:
            handlers_map: Maps handler names to their task-level configurations
            tasks_map: Maps task names to their handler-level configurations
        """
        
        if handlers and tasks:
            return self.logger_handlers_map, self.logger_tasks_map
        elif handlers:
            return self.logger_handlers_map
        elif tasks:
            return self.logger_tasks_map