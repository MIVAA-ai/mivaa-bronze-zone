import logging
import os
from colorama import Fore, Style, init

# Initialize colorama for Windows compatibility
init(autoreset=True)

create_file: bool = False

def configure_logger(log_file_name: str):
    """
    Configures and returns a logger instance with optional file logging and colored console output.

    Parameters:
    - log_file_name (str): Name of the file to log messages.
    - create_file (bool): Whether to create the log file if it doesn't exist.

    Returns:
    - logging.Logger: Configured logger instance.
    """
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)

    # Prevent adding multiple handlers if the logger is reused
    if not logger.handlers:
        if create_file and not os.path.exists(log_file_name):
            # Create the log file directory if it doesn't exist
            os.makedirs(os.path.dirname(log_file_name), exist_ok=True)

        # File handler (only if create_file is True)
        if create_file:
            file_handler = logging.FileHandler(log_file_name)
            file_handler.setLevel(logging.INFO)
            file_formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
            file_handler.setFormatter(file_formatter)
            logger.addHandler(file_handler)

        # Stream handler with color-coded log levels
        class ColoredFormatter(logging.Formatter):
            LEVEL_COLORS = {
                "DEBUG": Fore.BLUE,
                "INFO": Fore.GREEN,
                "WARNING": Fore.YELLOW,
                "ERROR": Fore.RED,
                "CRITICAL": Fore.MAGENTA,
            }

            def format(self, record):
                level_color = self.LEVEL_COLORS.get(record.levelname, "")
                record.levelname = f"{level_color}{record.levelname}{Style.RESET_ALL}"
                return super().format(record)

        stream_handler = logging.StreamHandler()
        stream_handler.setLevel(logging.INFO)
        stream_formatter = ColoredFormatter("%(asctime)s - %(levelname)s - %(message)s")
        stream_handler.setFormatter(stream_formatter)
        logger.addHandler(stream_handler)

    return logger
