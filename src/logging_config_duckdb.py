
import logging
import time
from pathlib import Path

def setup_logging(name: str = None) -> logging.Logger:
    """
    Setup logging to file and console.
    Logs are saved in 'logs/' directory with timestamp.
    Configures the ROOT logger so all modules log to the file.
    """
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.INFO)
    
    # Avoid adding handlers multiple times
    if root_logger.hasHandlers():
        return logging.getLogger(name)
    
    # Create logs directory if it doesn't exist
    log_dir = Path("logs")
    log_dir.mkdir(exist_ok=True)
    
    # Timestamped log file
    timestamp = time.strftime("%Y%m%d_%H%M%S")
    log_file = log_dir / f"duckdb_shortcuts_{timestamp}.log"
    
    # Formatter
    formatter = logging.Formatter(
        '[%(levelname)-7s] %(asctime)s - %(name)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    # File Handler
    file_handler = logging.FileHandler(log_file)
    file_handler.setFormatter(formatter)
    root_logger.addHandler(file_handler)
    
    # Console Handler
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    root_logger.addHandler(console_handler)
    
    return logging.getLogger(name)

def log_section(logger: logging.Logger, title: str, width: int = 60):
    """Log a section header with separators."""
    separator = "=" * width
    logger.info(separator)
    logger.info(title)
    logger.info(separator)

def log_dict(logger: logging.Logger, data: dict, title: str = None):
    """Log dictionary as formatted key-value pairs."""
    if title:
        logger.info(f"--- {title} ---")
    if not data:
        return
    max_key_len = max(len(str(k)) for k in data.keys())
    for key, value in data.items():
        logger.info(f"{str(key).ljust(max_key_len)} : {value}")

