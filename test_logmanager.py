from logmanager import LogManager
from loguru import logger

def main():
    #################################### WITHOUT CONFIG FILE ####################################
    # # If config_file is not provided, LogManager will use default settings
    # # The following is acceptable:
    # LogManager()
    # # Example logging calls
    # # Note: if LogManager(), the default logger_task is "main"
    # # Logger task has to be specified during logging calls
    # logger_main = logger.bind(logger_task="main")
    # logger_main.debug("This debug message will not print.", logger_task="main2")
    # logger_main.info("This is an info message from the main task.")
    # logger_main.warning("This is a warning message from the main task.")
    # logger_main.error("This is an error message from the main task.")
    # logger_main.success("This is a success message from the main task.")

    # # The following is also acceptable:
    # LogManager(log_dir="./my_logs", name="logger_name", task="my_task")
    # # Example logging calls
    # logger_my_task = logger.bind(logger_task="my_task")
    # logger_my_task.debug("This is a debug message from my_task.")
    # logger_my_task.info("This is an info message from my_task.")
    # logger_my_task.warning("This is a warning message from my_task.")
    # logger_my_task.error("This is an error message from my_task.")
    # logger_my_task.success("This is a success message from my_task.")

    #################################### WITH CONFIG FILE ####################################
    # If config_file is provided, LogManager will use the settings defined in the config file
    config_file_path = "example_config.yaml"
    log_manager = LogManager(config_file=config_file_path)

    # Get loggers
    logger_main = logger.bind(logger_task="main")
    logger_background = logger.bind(logger_task="background")

    # Get mappings
    handlers_map, tasks_map = log_manager.get_mappings()
    print("Handlers Map:", handlers_map)
    print("Tasks Map:", tasks_map)

    # Example logging calls
    logger_main.debug("This debug message should print to console .") # Since console level is set to WARNING in config
    logger_main.info("This info message should only print to console.") # Since file level is set to WARNING in config
    logger_main.critical("This critical message should print to console and file.")
    logger_background.debug("This debug message should NOT print to console.") # Since level is set to INFO in config
    logger_background.error("This error message should print to console.")

    # Add new tasks
    print("=" * 20, "ADDING NEW TASKS", "=" * 20)
    logger_additional = logger.bind(logger_task="additional")
    log_manager.add_task("additional", [("console", "INFO")])
    logger_additional.debug("This debug message should NOT print to console.")
    logger_additional.info("This info message should print to console.")
    logger_additional.error("This error message should also print to console.")

    # Get updated mappings
    handlers_map, tasks_map = log_manager.get_mappings()
    print("Updated Handlers Map:", handlers_map)
    print("Updated Tasks Map:", tasks_map)

    # Update existing tasks
    print("=" * 20, "UPDATING EXISTING TASKS", "=" * 20)
    log_manager.add_task("additional", [("console", "DEBUG")])
    logger_additional.debug("This debug message should now print to console.")
    logger_additional.info("This info message should print to console.")
    logger_additional.error("This error message should also print to console.")

    # Get updated mappings after adding new task
    handlers_map, tasks_map = log_manager.get_mappings()
    print("Updated Handlers Map after adding new task:", handlers_map)
    print("Updated Tasks Map after adding new task:", tasks_map)

    # Remove handlers
    print("=" * 20, "REMOVING HANDLERS", "=" * 20)
    log_manager.remove_handler_by_name("console")
    logger_additional.debug("This debug message should NOT print to console anymore.")
    logger_additional.info("This info message should NOT print to console anymore.")
    logger_additional.error("This error message should NOT print to console anymore.")

    # Get mappings after removing handlers
    handlers_map, tasks_map = log_manager.get_mappings()
    print("Handlers Map after removing handlers:", handlers_map)
    print("Tasks Map after removing handlers:", tasks_map)

    print("=" * 20, "END", "=" * 20)

if __name__ == "__main__":
    main()

