import os
from datetime import datetime
from pathlib import Path
import threading
import queue

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

class ThreadedLogger:
    def __init__(self, name="default", log_to_file=True, log_to_console=True, log_directory=LOG_DIRECTORY, use_ansi_colors=False, batch_size=30):
        self.name = name
        self.log_to_file = log_to_file
        self.log_to_console = log_to_console
        self.log_directory = log_directory
        self.use_ansi_colors = use_ansi_colors
        self.log_file_path = None
        self.batch_size = batch_size

        # Queue and threading
        self.log_queue = queue.Queue()
        self.stop_event = threading.Event()
        self.thread = threading.Thread(target=self._process_queue)
        self.thread.daemon = True  # Ensure the thread exits with the main program
        self.thread.start()

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

    def _process_queue(self):
        """Continuously process log messages from the queue."""
        buffer = []
        while not self.stop_event.is_set():
            try:
                # Get a log message from the queue
                log_message = self.log_queue.get(timeout=0.1)
                buffer.append(log_message)
                # If the buffer is full, flush it
                if len(buffer) >= self.batch_size:
                    self._flush_buffer(buffer)
                    buffer.clear()
                self.log_queue.task_done()
            except queue.Empty:
                # Flush remaining logs in the buffer if queue is empty
                if buffer:
                    self._flush_buffer(buffer)
                    buffer.clear()

    def _flush_buffer(self, buffer):
        """Write a batch of log messages to the file."""
        if self.log_to_file and self.log_file_path:
            with open(self.log_file_path, "a") as file:
                for message, level, kwargs in buffer:
                    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    formatted_message = f"[{timestamp}] [{level}] {message}"
                    file.write(formatted_message + "\n")

    def log_message(self, message, level="INFO", **kwargs):
        """Enqueue the log message instead of processing it directly."""
        self.log_queue.put((message, level, kwargs))

        # Print to console immediately if enabled
        if self.log_to_console:
            timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            formatted_message = f"[{timestamp}] [{level}] {message}"
            color = COLORS.get(level, "") if self.use_ansi_colors else ""
            reset = COLORS["RESET"] if self.use_ansi_colors else ""
            print(f"{color}{formatted_message}{reset}")

    def info(self, message, **kwargs):
        """Log an informational message."""
        self.log_message(message, level="INFO", **kwargs)

    def warning(self, message, **kwargs):
        """Log a warning message."""
        self.log_message(message, level="WARNING", **kwargs)

    def error(self, message, **kwargs):
        """Log an error message."""
        self.log_message(message, level="ERROR", **kwargs)

    def close(self):
        """Ensure proper shutdown of the logger thread."""
        self.stop_event.set()
        self.thread.join()
        # Flush any remaining logs in the queue
        while not self.log_queue.empty():
            log_message = self.log_queue.get()
            self._flush_buffer([log_message])
            self.log_queue.task_done()

def create_threaded_logger(name, log_directory="./logs", log_to_console=True, log_to_file=True, use_ansi_colors=False, batch_size=10):
    log_directory = Path(log_directory)
    log_directory.mkdir(parents=True, exist_ok=True)

    return ThreadedLogger(
        name=name,
        log_to_file=log_to_file,
        log_to_console=log_to_console,
        log_directory=log_directory,
        use_ansi_colors=use_ansi_colors,
        batch_size=batch_size
    )
