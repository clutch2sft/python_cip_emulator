import os
from datetime import datetime
import logging
from pathlib import Path

# Default log directory and log file name format
LOG_DIRECTORY = "./logs"
LOG_FILE_NAME_FORMAT = "{name}_log_{timestamp}.txt"

# ANSI escape codes for colored terminal output (for Unix-based systems)
COLORS = {
    "INFO": "\033[94m",       # Blue
    "WARNING": "\033[93m",    # Yellow
    "ERROR": "\033[91m",      # Red
    "RESET": "\033[0m"        # Reset color
}

class Logger:
    def __init__(self, name="default", log_to_file=True, log_to_console=True, log_directory=LOG_DIRECTORY, use_ansi_colors=False):
        self.name = name
        self.log_to_file = log_to_file
        self.log_to_console = log_to_console
        self.log_directory = log_directory
        self.use_ansi_colors = use_ansi_colors  # Determines if ANSI colors are applied
        self.log_file_path = None

        if self.log_to_file:
            self._initialize_log_file()

    def _initialize_log_file(self):
        os.makedirs(self.log_directory, exist_ok=True)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        self.log_file_path = os.path.join(
            self.log_directory, 
            LOG_FILE_NAME_FORMAT.format(name=self.name, timestamp=timestamp)
        )
        with open(self.log_file_path, "a") as file:
            file.write(f"Log started at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            file.write("=" * 40 + "\n")

    def log_message(self, message, level="INFO", **kwargs):
        tag = kwargs.get("tag", None)
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        formatted_message = f"[{timestamp}] [{level}] {message}"

        if self.log_to_console:
            color = COLORS.get(level, "") if self.use_ansi_colors else ""
            reset = COLORS["RESET"] if self.use_ansi_colors else ""
            print(f"{color}{formatted_message}{reset}")

        if self.log_to_file and self.log_file_path:
            with open(self.log_file_path, "a") as file:
                file.write(formatted_message + "\n")

    def info(self, message, **kwargs):
        """Log an informational message."""
        self.log_message(message, level="INFO", **kwargs)

    def warning(self, message, **kwargs):
        """Log a warning message."""
        self.log_message(message, level="WARNING", **kwargs)

    def error(self, message, **kwargs):
        """Log an error message."""
        self.log_message(message, level="ERROR", **kwargs)

def create_logger(name, log_directory="./logs", log_to_console=True, log_to_file=True, use_ansi_colors=False):
    log_directory = Path(log_directory)
    log_directory.mkdir(parents=True, exist_ok=True)

    return Logger(
        name=name, 
        log_to_file=log_to_file, 
        log_to_console=log_to_console, 
        log_directory=log_directory, 
        use_ansi_colors=use_ansi_colors
    )
