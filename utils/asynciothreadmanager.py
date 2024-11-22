import asyncio
from concurrent.futures import ThreadPoolExecutor
import threading


class NamedThreadPoolExecutor(ThreadPoolExecutor):
    """ThreadPoolExecutor with named threads for asyncio."""
    def __init__(self, max_workers=None, thread_name_prefix="asyncio_worker"):
        super().__init__(max_workers=max_workers, thread_name_prefix=thread_name_prefix)
        self._thread_name_prefix = thread_name_prefix  # Ensure prefix is explicitly set

    def _adjust_thread_count(self):
        """Override to name threads."""
        if len(self._threads) < self._max_workers:
            thread_name = f"{self._thread_name_prefix}_{len(self._threads) + 1}"
            print(f"Starting thread: {thread_name}")  # Debug print
            thread = threading.Thread(
                target=self._worker,
                name=thread_name,
                daemon=True
            )
            thread.start()
            self._threads.add(thread)



class AsyncioThreadManager:
    """Utility to create and manage custom event loops with named threads."""
    def __init__(self, thread_name_prefix="NamedAsyncioThread", max_workers=10):
        self.thread_name_prefix = thread_name_prefix
        self.max_workers = max_workers
        self.loop = None
        self.loop_thread = None

    def create_event_loop(self):
        """Create a new asyncio event loop with a named ThreadPoolExecutor."""
        self.loop = asyncio.new_event_loop()
        executor = NamedThreadPoolExecutor(max_workers=self.max_workers, thread_name_prefix=self.thread_name_prefix)
        self.loop.set_default_executor(executor)
        return self.loop

    def start_event_loop(self):
        """Start the event loop in a separate thread."""
        if not self.loop:
            self.create_event_loop()
        self.loop_thread = threading.Thread(
            target=self._run_event_loop,
            name=f"{self.thread_name_prefix}_EventLoopThread",
            daemon=True
        )
        self.loop_thread.start()
        print(f"{self.thread_name_prefix}: Event loop started in thread {self.loop_thread.name}")  # Debug print

    def _run_event_loop(self):
        """Run the event loop."""
        asyncio.set_event_loop(self.loop)
        print(f"{self.thread_name_prefix}: Running event loop.")  # Debug print
        self.loop.run_forever()

    def run_until_complete(self, coro):
        """Run a coroutine until completion using the managed event loop."""
        if not self.loop:
            self.create_event_loop()
        asyncio.set_event_loop(self.loop)
        return self.loop.run_until_complete(coro)

    def shutdown(self):
        """Shut down the event loop and clean up."""
        if self.loop:
            # Stop the event loop
            self.loop.call_soon_threadsafe(self.loop.stop)

            # Wait for the loop thread to finish
            if self.loop_thread and self.loop_thread.is_alive():
                self.loop_thread.join()

            # Clean up any remaining asynchronous generators
            asyncio.set_event_loop(self.loop)
            self.loop.run_until_complete(self.loop.shutdown_asyncgens())

            # Close the loop
            self.loop.close()
            self.loop = None
            print(f"{self.thread_name_prefix}: Event loop shut down.")  # Debug print
