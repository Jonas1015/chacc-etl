import logging
import os
from config import LOG_LEVEL, LOG_PATH, LOGS_DIR

def setup_logging():
    """
    Setup logging configuration for the ETL pipeline.
    """
    os.makedirs(LOGS_DIR, exist_ok=True)

    logger = logging.getLogger()
    logger.setLevel(getattr(logging, LOG_LEVEL))

    for handler in logger.handlers[:]:
        logger.removeHandler(handler)

    file_formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    console_formatter = logging.Formatter(
        '%(levelname)s - %(name)s - %(message)s'
    )

    file_handler = logging.FileHandler(LOG_PATH)
    file_handler.setLevel(getattr(logging, LOG_LEVEL))
    file_handler.setFormatter(file_formatter)

    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(console_formatter)

    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

    return logger

def get_task_logger(task_name):
    """
    Get a logger for a specific task.
    """
    return logging.getLogger(f"etl.{task_name}")

def log_task_start(task):
    """
    Log the start of a task.
    """
    logger = get_task_logger(task.__class__.__name__)
    logger.info(f"Starting task: {task.task_id}")

def log_task_complete(task):
    """
    Log the completion of a task.
    """
    logger = get_task_logger(task.__class__.__name__)
    logger.info(f"Completed task: {task.task_id}")

def log_task_error(task, error):
    """
    Log an error in a task.
    """
    logger = get_task_logger(task.__class__.__name__)
    logger.error(f"Task failed: {task.task_id} - {error}")