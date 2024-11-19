import os
import asyncio
from datetime import datetime
from collections import deque
import threading
import aiofiles
import inspect
from threading import Lock

COLORS = {
    "INFO": "\033[94m",  # Blue
    "WARNING": "\033[93m",  # Yellow
    "ERROR": "\033[91m",  # Red
    "RESET": "\033[0m",  # Reset color
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
        self._shutdown_complete = False  # Track shutdown completion

        self.log_queue = deque()
        self.queue_lock = Lock()
        self.stop_event = threading.Event()

        # Async condition for message notification
        self.condition = asyncio.Condition()

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
        """Start the asyncio event loop."""
        asyncio.set_event_loop(self.loop)
        try:
            self.loop.run_until_complete(self._process_queue())
        except asyncio.CancelledError:
            pass  # Loop canceled during shutdown
        finally:
            self.loop.close()  # Close loop when done

    async def _process_queue(self):
        """Processes the log queue and flushes to file in batches."""
        buffer = []
        last_flush_time = self.loop.time()

        while not self.stop_event.is_set():
            try:
                async with self.condition:
                    await asyncio.wait_for(self.condition.wait(), timeout=self.flush_interval)
            except asyncio.TimeoutError:
                pass  # Timeout is expected; proceed to flush if needed
            except asyncio.CancelledError:
                break  # Allow graceful shutdown on cancellation

            # Process messages from the queue
            while self.log_queue:
                with self.queue_lock:
                    buffer.append(self.log_queue.popleft())
                if len(buffer) >= self.batch_size:
                    await self._flush_buffer(buffer)
                    buffer.clear()
                    last_flush_time = self.loop.time()

            # Flush buffer if timeout has elapsed
            if buffer and self.loop.time() - last_flush_time >= self.flush_interval:
                await self._flush_buffer(buffer)
                buffer.clear()
                last_flush_time = self.loop.time()

        # Ensure all remaining logs are flushed
        if buffer:
            await self._flush_buffer(buffer)

    async def _flush_buffer(self, buffer):
        """Asynchronously write a batch of log messages to the file."""
        if self.log_to_file and self.log_file_path:
            async with aiofiles.open(self.log_file_path, "a") as file:
                timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                log_lines = [
                    f"[{timestamp}] [Line: {lineno}] [{level}] {message}\n"
                    for message, level, lineno in buffer
                ]
                await file.write("".join(log_lines))

    def log_message(self, message, level="INFO", log_to_console_cancel=False):
        """Add a log message to the queue."""
        if self._shutdown_complete:
            return  # Ignore log messages after shutdown starts

        frame = inspect.currentframe().f_back.f_back
        lineno = frame.f_lineno

        if self.log_to_console and not log_to_console_cancel:
            timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            formatted_message = f"[{timestamp}] [{level}] {message} [Line: {lineno}]"
            color = COLORS.get(level, "") if self.use_ansi_colors else ""
            reset = COLORS["RESET"] if self.use_ansi_colors else ""
            print(f"{color}{formatted_message}{reset}")

        # Add message to the queue
        with self.queue_lock:
            self.log_queue.append((message, level, lineno))

        # Notify the condition if the logger loop is running
        if not self._shutdown_complete and self.loop.is_running():
            try:
                asyncio.run_coroutine_threadsafe(self._notify_condition(), self.loop)
            except RuntimeError:
                pass  # Ignore if loop is no longer running

    async def _notify_condition(self):
        """Notify the logger's asyncio condition to unblock any waiting coroutines."""
        async with self.condition:
            self.condition.notify_all()

    def info(self, message, log_to_console_cancel=False):
        self.log_message(message, level="INFO", log_to_console_cancel=log_to_console_cancel)

    def warning(self, message, log_to_console_cancel=False):
        self.log_message(message, level="WARNING", log_to_console_cancel=log_to_console_cancel)

    def error(self, message, log_to_console_cancel=False):
        self.log_message(message, level="ERROR", log_to_console_cancel=log_to_console_cancel)

    async def shutdown(self):
        """Flush all logs, stop the logger thread, and close the asyncio loop."""
        if self._shutdown_complete:
            return

        self._shutdown_complete = True
        self.stop_event.set()  # Signal thread to stop

        # Cancel all tasks in the logger's event loop
        if self.loop.is_running():
            tasks = [t for t in asyncio.all_tasks(self.loop) if not t.done()]
            for task in tasks:
                task.cancel()  # Cancel tasks
                try:
                    await task
                except asyncio.CancelledError:
                    pass  # Ignore expected cancellation

            # Stop the loop safely
            self.loop.call_soon_threadsafe(self.loop.stop)

        # Ensure the thread exits
        self.thread.join()

    def flush_and_stop(self):
        """Flush remaining logs and stop the logger gracefully."""
        asyncio.run(self.shutdown())  # Clean shutdown of the logger

def create_threaded_logger(
    name,
    log_directory="./logs",
    log_to_console=True,
    log_to_file=True,
    use_ansi_colors=False,
    batch_size=10,
    flush_interval=0.5
):
    """Factory method to create and configure a ThreadedLogger."""
    os.makedirs(log_directory, exist_ok=True)
    return ThreadedLogger(
        name=name,
        log_to_file=log_to_file,
        log_to_console=log_to_console,
        use_ansi_colors=use_ansi_colors,
        log_directory=log_directory,
        batch_size=batch_size,
        flush_interval=flush_interval,
    )
