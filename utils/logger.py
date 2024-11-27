import os
import asyncio
from datetime import datetime
from collections import deque
import inspect
import aiofiles
import threading
from threading import Lock
from core.component_base import ComponentBase

COLORS = {
    "INFO": "\033[94m",    # Blue
    "WARNING": "\033[93m", # Yellow
    "ERROR": "\033[91m",   # Red
    "DEBUG": "\033[92m",   # Green
    "RESET": "\033[0m",    # Reset color
}



class ThreadedLogger(ComponentBase):
    def __init__(self, thread_manager, name="default", log_to_file=True, log_to_console=True, use_ansi_colors=False, log_directory="./logs", batch_size=75, flush_interval=1, debug=False):
        """
        Initialize the ThreadedLogger.
        Args:
            thread_manager (AsyncioThreadManager): Shared thread manager for task scheduling.
        """
        super().__init__(name=name, logger=self._null_logger())  # Loggers don't need an internal logger
        self.thread_manager = thread_manager  # Use the passed-in thread manager
        self.log_to_file = log_to_file
        self.log_to_console = log_to_console
        self.use_ansi_colors = use_ansi_colors
        self.log_directory = log_directory
        self.batch_size = batch_size
        self.flush_interval = flush_interval
        self.log_file_path = None
        self._shutdown_complete = False
        self.debug = debug
        self.name = name

        self.log_queue = deque()
        self.queue_lock = Lock()
        self.condition = asyncio.Condition()

    async def start(self):
        """Start the logger."""
        await super().start()
        self._shutdown_complete = False

        # Initialize log file if required
        if self.log_to_file:
            self._initialize_log_file()

        # Create the logging task with the thread manager's loop
        self._task = asyncio.run_coroutine_threadsafe(
            self._process_queue(),
            self.thread_manager.loop
        )
        self.info(f"Logger {self.name} started.")
        # for task in asyncio.all_tasks():
        #     print(f"Logger ln57 Task Name: {task.get_name()}")

    async def stop(self):
        """Stop the logger gracefully."""
        if self._shutdown_complete:
            return
        self._shutdown_complete = True
        self.info(f"Logger {self.name} stopping...")

        # Signal processing loop to exit
        async with self.condition:
            self.condition.notify_all()

        # Cancel and await the logging task
        if hasattr(self, "_task"):
            self._task.cancel()
            try:
                await asyncio.gather(self._task, return_exceptions=True)
            except asyncio.CancelledError:
                self.info(f"Logger {self.name}: Logging task cancelled during shutdown.")
            except Exception as e:
                self.error(f"Logger {self.name}: Error during shutdown: {e}")


    def _initialize_log_file(self):
        """Create the log file if logging to file is enabled."""
        try:
            self.debug_print(f"Initializing log file for {self.name}...")  # Debug: Starting initialization

            # Ensure the directory exists
            os.makedirs(self.log_directory, exist_ok=True)
            self.debug_print(f"Log directory ensured at: {self.log_directory}")  # Debug

            # Generate the timestamped log file path
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            self.log_file_path = os.path.join(self.log_directory, f"{self.name}_log_{timestamp}.txt")
            self.debug_print(f"Log file path set to: {self.log_file_path}")  # Debug

            # Verify file path creation
            with open(self.log_file_path, "a") as f:
                f.write("")  # Create an empty file if it doesn't exist
            self.debug_print(f"Log file initialized successfully at: {self.log_file_path}")  # Debug

        except Exception as e:
            self.debug_print(f"Error initializing log file for {self.name}: {e}")  # Debug
            self.logger.error(f"Failed to initialize log file for {self.name}: {e}")
            raise

    async def _process_queue(self):
        """Processes the log queue and flushes to file in batches."""
        buffer = []
        last_flush_time = asyncio.get_running_loop().time()

        while not self._shutdown_complete or self.log_queue:
            try:
                # Wait for new log messages or flush interval
                async with self.condition:
                    if len(self.log_queue) > 0:
                        self.debug_print(f"{self.name} ln118 Log queue size: {len(self.log_queue)}")
                    await asyncio.wait_for(self.condition.wait(), timeout=self.flush_interval)
            except asyncio.TimeoutError:
                pass  # Proceed to flush logs
            except asyncio.CancelledError:
                break

            # Process the queue
            while self.log_queue:
                with self.queue_lock:
                    buffer.append(self.log_queue.popleft())
                if len(buffer) >= self.batch_size:
                    await self._flush_buffer(buffer)
                    buffer.clear()
                    last_flush_time = asyncio.get_running_loop().time()

            # Flush remaining logs after timeout
            if buffer and asyncio.get_running_loop().time() - last_flush_time >= self.flush_interval:
                self.debug_print(f"{self.name} ls 135 Flushing {len(buffer)} log entries to file...")
                await self._flush_buffer(buffer)
                self.debug_print(f"{self.name} ln137 Flush complete.")
                buffer.clear()
                last_flush_time = asyncio.get_running_loop().time()

        # Flush any remaining logs
        if buffer:
            self.debug_print(f"{self.name} ln 143 Flushing {len(buffer)} log entries to file...")
            await self._flush_buffer(buffer)
            self.debug_print(f"{self.name} ln 145 Flush complete.")

    async def _flush_buffer(self, buffer):
        """Write a batch of log messages to the log file."""
        self.debug_print(f"{self.name} ln 151 Log2File {self.log_to_file}, logfpath:{self.log_file_path}.")
        if self.log_to_file and self.log_file_path:
            self.debug_print(f"{self.name} ln 153 flushing buffer.")
            try:
                async with aiofiles.open(self.log_file_path, "a") as file:
                    log_lines = []

                    for entry in buffer:
                        try:
                            # Unpack the full structure: (message, level, filename, func_name, lineno)
                            message, level, filename, func_name, lineno = entry
                            timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                            log_lines.append(
                                f"[{timestamp}] [File: {filename}] [Function: {func_name}] [Line: {lineno}] [{level}] {message}\n"
                            )
                        except ValueError as e:
                            self.debug_print(f"Logger {self.name}: Malformed log entry {entry}: {e}")
                        except asyncio.CancelledError:
                            self.debug_print(f"Logger {self.name}: Task cancelled while processing entry: {entry}")
                            break
                    if log_lines:
                        await file.write("".join(log_lines))
            except asyncio.CancelledError:
                self.debug_print(f"Logger {self.name}: Task cancelled during file writing.")
                # Allow graceful cancellation
                raise
            except Exception as e:
                self.debug_print(f"Logger {self.name}: Error writing to log file: {e}")


    def log_message(self, message, level="INFO"):
        """Add a log message to the queue."""
        if self._shutdown_complete:
            return  # Ignore log messages after shutdown starts

        # Use the inspect module to capture context
        frame = inspect.currentframe().f_back.f_back
        lineno = frame.f_lineno
        filename = frame.f_code.co_filename
        func_name = frame.f_code.co_name

        if self.log_to_console:
            timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            formatted_message = (
                f"[{timestamp}] [{level}] {message} [File: {filename}] "
                f"[Function: {func_name}] [Line: {lineno}]"
            )
            color = COLORS.get(level, "") if self.use_ansi_colors else ""
            reset = COLORS["RESET"] if self.use_ansi_colors else ""
            print(f"{color}{formatted_message}{reset}")

        # Add message to the queue
        with self.queue_lock:
            self.log_queue.append((message, level, filename, func_name, lineno))

        # Notify the logger's asyncio condition
        if not self._shutdown_complete:
            asyncio.run_coroutine_threadsafe(self._notify_condition(), self.thread_manager.loop)

    async def _notify_condition(self):
        """Notify the logger's asyncio condition."""
        async with self.condition:
            self.condition.notify_all()

    def info(self, message):
        self.log_message(message, level="INFO")

    def warning(self, message):
        self.log_message(message, level="WARNING")

    def error(self, message):
        self.log_message(message, level="ERROR")

    def debug(self, message):
        if self.debug:
            self.log_message(message, level="DEBUG")

    def debug_print(self, message):
        """
        Prints debug messages to the console if self.debug is enabled.
        Dynamically retrieves the calling line number and function name.
        
        Args:
            message (str): The debug message to print.
        """
        if self.debug:
            # Get the frame of the caller
            frame = inspect.currentframe().f_back
            line_number = frame.f_lineno
            function_name = frame.f_code.co_name
            
            print(f"[DEBUG:][{self.name}] {function_name} ln{line_number}: {message}")



def create_threaded_logger(
    threadmanager,
    name,
    log_directory="./logs",
    log_to_console=True,
    log_to_file=True,
    use_ansi_colors=False,
    batch_size=10,
    flush_interval=0.5,
    debug=False
):
    """Factory method to create and configure a ThreadedLogger."""
    os.makedirs(log_directory, exist_ok=True)
    return ThreadedLogger(
        threadmanager,
        name=name,
        log_to_file=log_to_file,
        log_to_console=log_to_console,
        use_ansi_colors=use_ansi_colors,
        log_directory=log_directory,
        batch_size=batch_size,
        flush_interval=flush_interval,
        debug= debug,
    )
