import os
import asyncio
from datetime import datetime
from collections import deque
import threading
import aiofiles
import inspect  # For retrieving the caller's line number
from threading import Lock

# ANSI escape codes for colored terminal output (for Unix-based systems)
COLORS = {
    "INFO": "\033[94m",       # Blue
    "WARNING": "\033[93m",    # Yellow
    "ERROR": "\033[91m",      # Red
    "RESET": "\033[0m"        # Reset color
}

class ThreadedLogger:
    def __init__(self, name="default", log_to_file=True, log_to_console=True, use_ansi_colors=False, log_directory="./logs", batch_size=75, flush_interval=1):
        self.name = name
        self.log_to_file = log_to_file
        self.log_to_console = log_to_console
        self.use_ansi_colors = use_ansi_colors
        self.log_directory = log_directory
        self.batch_size = batch_size
        self.flush_interval = flush_interval
        self.log_file_path = None

        self.log_queue = deque()
        self.queue_lock = Lock()

        # Start the asyncio event loop in a separate thread
        self.loop = asyncio.new_event_loop()
        self.thread = threading.Thread(target=self._start_event_loop, daemon=True)
        self.thread.start()

        # Initialize the log file if needed
        if self.log_to_file:
            self._initialize_log_file()

    def _initialize_log_file(self):
        os.makedirs(self.log_directory, exist_ok=True)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        self.log_file_path = os.path.join(self.log_directory, f"{self.name}_log_{timestamp}.txt")

    def _start_event_loop(self):
        asyncio.set_event_loop(self.loop)
        self.loop.run_until_complete(self._process_queue())

    async def _process_queue(self):
        buffer = []
        last_flush_time = asyncio.get_event_loop().time()
        condition = asyncio.Condition()

        async def wait_for_messages():
            async with condition:
                await condition.wait()

        while not self.stop_event.is_set():
            # Wait for new log messages or flush timeout
            if not self.log_queue:
                await asyncio.wait_for(wait_for_messages(), timeout=self.flush_interval)

            # Process messages from the queue
            while self.log_queue:
                buffer.append(self.log_queue.popleft())
                if len(buffer) >= self.batch_size:
                    await self._flush_buffer(buffer)
                    buffer.clear()
                    last_flush_time = asyncio.get_event_loop().time()

            # Flush buffer if timeout has elapsed
            if buffer and asyncio.get_event_loop().time() - last_flush_time >= self.flush_interval:
                await self._flush_buffer(buffer)
                buffer.clear()
                last_flush_time = asyncio.get_event_loop().time()

    async def _flush_buffer(self, buffer):
        """Asynchronously write a batch of log messages to the file."""
        if self.log_to_file and self.log_file_path:
            timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            async with aiofiles.open(self.log_file_path, "a") as file:
                log_lines = [
                    f"[{timestamp}] [Line: {lineno}] [{level}] {message}\n"
                    for message, level, lineno in buffer
                ]
                await file.write("".join(log_lines))

    def log_message(self, message, level="INFO", log_to_console_cancel=False):
        """Queue a log message for asynchronous processing and optionally print to console."""
        lineno = inspect.currentframe().f_back.f_back.f_lineno

        if self.log_to_console and not log_to_console_cancel:
            timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            formatted_message = f"[{timestamp}] [Line: {lineno}] [{level}] {message}"
            color = COLORS.get(level, "") if self.use_ansi_colors else ""
            reset = COLORS["RESET"] if self.use_ansi_colors else ""
            print(f"{color}{formatted_message}{reset}")

        with self.queue_lock:
            self.log_queue.append((message, level, lineno))

    def info(self, message, log_to_console_cancel=False):
        self.log_message(message, level="INFO", log_to_console_cancel=log_to_console_cancel)

    def warning(self, message, log_to_console_cancel=False):
        self.log_message(message, level="WARNING", log_to_console_cancel=log_to_console_cancel)

    def error(self, message, log_to_console_cancel=False):
        self.log_message(message, level="ERROR", log_to_console_cancel=log_to_console_cancel)
        
    def close(self):
        """Stop the logger and ensure all pending messages are flushed."""
        self.stop_event.set()
        self.loop.call_soon_threadsafe(self.loop.stop)
        self.thread.join()
        if self.log_queue:
            asyncio.run(self._flush_buffer(list(self.log_queue)))


def create_threaded_logger(
    name,
    log_directory="./logs",
    log_to_console=True,
    log_to_file=True,
    use_ansi_colors=False,
    batch_size=10,
    flush_interval=0.5
):
    """
    Factory method to create and configure a ThreadedLogger.

    Args:
        name (str): The name of the logger.
        log_directory (str): The directory where log files will be stored.
        log_to_console (bool): Whether to log to the console.
        log_to_file (bool): Whether to log to a file.
        use_ansi_colors (bool): Whether to use ANSI escape codes for console output.
        batch_size (int): Number of messages to batch before flushing to the file.
        flush_interval (float): Maximum time in seconds before flushing a batch.

    Returns:
        ThreadedLogger: Configured logger instance.
    """
    # Ensure the log directory exists
    os.makedirs(log_directory, exist_ok=True)

    # Create and return a ThreadedLogger instance
    return ThreadedLogger(
        name=name,
        log_to_file=log_to_file,
        log_to_console=log_to_console,
        use_ansi_colors=use_ansi_colors,
        log_directory=log_directory,
        batch_size=batch_size,
        flush_interval=flush_interval,
    )
