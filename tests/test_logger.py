
from src import LogManager
from loguru import logger

def main():
    # ################################### WITHOUT CONFIG FILE ####################################
    # # If config_file is not provided, LogManager will use default settings
    # # The following is acceptable:
    # lm = LogManager()
    # # Example logging calls
    # # Note: if LogManager(), the default logger_task is "default_task"
    # logger_main = lm.get_logger("default_task")
    # logger_main.debug("This is a debug message from the default task.")
    # logger_main.info("This is an info message from the default task.")
    # logger_main.warning("This is a warning message from the default task.")
    # logger_main.error("This is an error message from the default task.")
    # logger_main.success("This is a success message from the default task.")

    # # The following is also acceptable:
    # lm = LogManager(timezone="Antartica/South_Pole")
    # # Example logging calls
    # # Note: if LogManager(), the default logger_task is "default_task"
    # logger_main = lm.get_logger("default_task")
    # logger_main.debug("This is a debug message from default_task.")
    # logger_main.info("This is an info message from default_task.")
    # logger_main.warning("This is a warning message from default_task.")
    # logger_main.error("This is an error message from default_task.")
    # logger_main.success("This is a success message from default_task.")

    #################################### WITH CONFIG FILE ####################################
    config_file = "./src/main/example_config.yaml"
    lm = LogManager(config_path=config_file)

    # Get loggers
    logger_A = lm.get_logger("logger_a")
    logger_B = lm.get_logger("logger_b")

    try:
        logger_C = lm.get_logger("logger_c") # This will raise an error since logger_c is not defined in the config file
    except Exception as e:
        print(f"EXCEPTION - {e}")

    # Example logging calls
    logger_A.debug("This debug message should NOT print to console.") # Since level is set to INFO in config 
    logger_A.info("This info message should only print to console.") # Since file level is set to WARNING in config
    logger_A.critical("This critical message should print to console and file.")
    logger_B.debug("This debug message should NOT print to console.") # Since level is set to INFO in config
    logger_B.critical("This critical message should print to console, but for logger_b.")

    # Add new logger and handler
    print("=" * 40, "ADD NEW LOGGER AND HANDLER", "=" * 40)
    lm.add_handler(
        "handler_console_fire",
        {
            "sink": "sys.stdout",
            "level": "info",
            "format": " 🔥 <green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | {extra[logger_name]} | {file: <16} | <cyan>{function}:{line}</cyan> - <level>{message}</level>",
            # "colorize": True,
        }
    )
    lm.add_logger("logger_c", [{'handler': 'handler_console_fire', 'level': 'debug'}])

    logger_C = lm.get_logger("logger_c")
    # Note: The handlers level has higher priority than the logger level.
    # so even if logger_c is set to debug, it will only print info and above to console.
    logger_C.debug("This debug message should NOT print to console.") # Since level is set to INFO in handler_console_fire
    logger_C.info("This info message should print to console.") # Since level is set to INFO in handler_console_fire
    logger_C.critical("This critical message should also print to console.")

    # The following format is also acceptable as long as it's defined in the config file under 'formats'
    lm.add_handler(
        "handler_console_fire2",
        {
            "sink": "sys.stdout",
            "level": "info",
            "format": "simple"
        }
    )

    # update handler
    print("=" * 40, "UPDATE HANDLER", "=" * 40)
    lm.update_handler(
        "handler_console_fire",
        {
            "sink": "sys.stdout",
            "level": "debug",
            "format": " 🧯 <green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | {extra[logger_name]} | {file: <16} | <cyan>{function}:{line}</cyan> - <level>{message}</level>",
        }
    )

    logger_C.debug("This debug message should print to console since the level is set to DEBUG in handler_console_fire.")
    logger_C.info("This info message should print to console as well.")
    logger_C.critical("This critical message should also print to console.")

    # update logger
    print("=" * 40, "UPDATE LOGGER", "=" * 40)
    lm.update_logger(
        "logger_c",
        [{'handler': 'handler_console_fire', 'level': 'ERROR'},
         {'handler': 'handler_console', 'level': 'error'}]
    )
    logger_C.debug("This debug message should NOT print to console since the level is set to ERROR for both handlers.")
    logger_C.error("This error message should print to console TWICE, 1 from handler_console_fire and 1 from handler_console.")

    # remove logger
    print("=" * 40, "REMOVE LOGGER", "=" * 40)
    lm.remove_logger("logger_c")
    logger_C.debug("This debug message should NOT print to console since logger_c is removed.")
    logger_C.error("This error message should NOT print to console since logger_c is removed.")
    logger_A.info("Only this info message should print to console from logger_A.")

    # remove handler
    print("=" * 40, "REMOVE HANDLER", "=" * 40)
    lm.remove_handler("handler_console")
    logger_A.debug("This debug message should not appear on the FILE")
    logger_A.info("This info message should not appear on the FILE")
    logger_A.error("This error message SHOULD appear on the FILE")

if __name__ == "__main__":
    main()

