import os
import asyncio
from datetime import datetime
from collections import deque
import threading
import aiofiles

class ThreadedLogger:
    def __init__(self, name="default", log_to_file=True, log_directory="./logs", batch_size=10, flush_interval=0.5):
        self.name = name
        self.log_to_file = log_to_file
        self.log_directory = log_directory
        self.batch_size = batch_size
        self.flush_interval = flush_interval
        self.log_file_path = None

        self.log_queue = deque()
        self.stop_event = threading.Event()

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

        while not self.stop_event.is_set():
            # Check for new log messages
            while self.log_queue:
                buffer.append(self.log_queue.popleft())
                if len(buffer) >= self.batch_size:
                    await self._flush_buffer(buffer)
                    buffer.clear()
                    last_flush_time = asyncio.get_event_loop().time()

            # Flush the buffer if the timeout has elapsed
            if buffer and asyncio.get_event_loop().time() - last_flush_time >= self.flush_interval:
                await self._flush_buffer(buffer)
                buffer.clear()
                last_flush_time = asyncio.get_event_loop().time()

            await asyncio.sleep(0.01)  # Prevent busy looping

    async def _flush_buffer(self, buffer):
        """Asynchronously write a batch of log messages to the file."""
        if self.log_to_file and self.log_file_path:
            async with aiofiles.open(self.log_file_path, "a") as file:
                for message, level in buffer:
                    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    formatted_message = f"[{timestamp}] [{level}] {message}\n"
                    await file.write(formatted_message)

    def log_message(self, message, level="INFO"):
        """Queue a log message for asynchronous processing."""
        self.log_queue.append((message, level))

    def info(self, message):
        self.log_message(message, level="INFO")

    def warning(self, message):
        self.log_message(message, level="WARNING")

    def error(self, message):
        self.log_message(message, level="ERROR")

    def close(self):
        """Stop the logger and ensure all pending messages are flushed."""
        self.stop_event.set()
        self.loop.call_soon_threadsafe(self.loop.stop)
        self.thread.join()
        if self.log_queue:
            asyncio.run(self._flush_buffer(list(self.log_queue)))
