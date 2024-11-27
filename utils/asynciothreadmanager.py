import asyncio
from concurrent.futures import ThreadPoolExecutor
import threading
import inspect
import traceback
import sys
import ctypes


class NamedThreadPoolExecutor(ThreadPoolExecutor):
    """ThreadPoolExecutor with named threads for asyncio and logging."""
    def __init__(self, max_workers=None, thread_name_prefix="asyncio_worker", debug=False):
        super().__init__(max_workers=max_workers, thread_name_prefix=thread_name_prefix)
        self.active_threads = set()  # Track active threads
        self._lock = threading.Lock()
        self.debug = True
        self.name = "NamedThreadPoolExecutor"
        self._thread_name_prefix = thread_name_prefix
        self._original_init_thread = threading.Thread.__init__  # Backup original thread init method

        # Patch threading.Thread to use custom init
        def custom_thread_init(instance, *args, **kwargs):
            # frame = inspect.currentframe()
            # depth = self.count_frames_to_top(frame)
            # last_frame = self.get_frame_by_depth(depth, frame)
            # del frame
            # line_number = last_frame.f_lineno
            # function_name = last_frame.f_code.co_name
            # self.debug_print(f"Called from {function_name} line {line_number}")
            # Get thread name from kwargs or fallback
            thread_name = kwargs.get("name", f"{self._thread_name_prefix}_{threading.active_count()}")
            kwargs["name"] = thread_name
            self._original_init_thread(instance, *args, **kwargs)  # Call original thread init method

            # Debug and track active threads
            self.debug_print(f"Initialized thread {thread_name}.")
            with self._lock:
                self.active_threads.add(thread_name)

            # Allow overriding the name dynamically if already set
            current_thread = threading.current_thread()
            if current_thread.name != thread_name:
                self.debug_print(f"Thread name dynamically set to {current_thread.name}.")
                with self._lock:
                    self.active_threads.discard(thread_name)
                    self.active_threads.add(current_thread.name)
        threading.Thread.__init__ = custom_thread_init  # Apply the patch

    def submit(self, fn, *args, **kwargs):
        """Submit a task and log its execution."""
        # Log the task submission
        self.debug_print(f"Task submitted: {fn.__name__ if hasattr(fn, '__name__') else fn}")
        self.debug_print(f"Submitted from: {fn.__qualname__}")
        # Wrap the function for logging and exception handling
        wrapped_fn = self._wrap_function(fn)
        return super().submit(wrapped_fn, *args, **kwargs)

    def _wrap_function(self, fn):
        """Wrap a function to log execution and exceptions."""
        def wrapped(*args, **kwargs):
            try:
                # self.debug_print(f"Running task: {fn.__name__ if hasattr(fn, '__name__') else fn}")
                return fn(*args, **kwargs)
            except Exception as e:
                self.debug_print(f"Task raised an exception: {e}")
                raise
        return wrapped

    def remove_thread(self, thread_name):
        with self._lock:
            self.active_threads.discard(thread_name)
        self.debug_print(f"Thread {thread_name} removed.")

    def shutdown(self, wait=True):
        """Shutdown the thread pool executor."""
        self.debug_print("Shutting down ThreadPoolExecutor...")
        
        # Collect active thread objects
        active_threads_before = threading.enumerate()
        self.debug_print(f"Active threads before shutdown: {[thread.name for thread in active_threads_before]}")

        # Cancel any remaining futures in the worker queue
        if hasattr(self, '_work_queue'):
            while not self._work_queue.empty():
                try:
                    task = self._work_queue.get_nowait()
                    self.debug_print(f"Cancelling pending task from queue: {task}")
                    if hasattr(task, 'cancel'):
                        task.cancel()
                except Exception as e:
                    self.debug_print(f"Error cancelling task: {e}")
            self.debug_print("Worker queue emptied.")
        else:
            self.debug_print("No work queue found.")

        # Attempt to terminate all threads except the main thread
        # for thread in active_threads_before:
        #     if thread.name == "MainCIPThread":
        #         self.debug_print(f"Skipping join for main thread: {thread.name}")
        #         continue

        #     if thread.is_alive():
        #         self.debug_print(f"Waiting for thread to finish: {thread.name}")
        #         try:
        #             thread.join(timeout=5)
        #             if thread.is_alive():
        #                 self.debug_print(f"Thread did not terminate: {thread.name}")
        #         except Exception as e:
        #             self.debug_print(f"Error while waiting for thread {thread.name}: {e}")
        for thread in active_threads_before:
            if thread.name == "MainCIPThread":
                self.debug_print(f"Skipping join for main thread: {thread.name}")
                continue

            if thread.is_alive():
                self.debug_print(f"Waiting for thread to finish: {thread.name}")
                
                # Log the stack trace
                if thread.ident in sys._current_frames():
                    self.debug_print(f"Thread {thread.name} stack trace:")
                    for line in traceback.format_stack(sys._current_frames()[thread.ident]):
                        self.debug_print(f"  {line.strip()}")

                # Signal the thread to stop if it uses a stop_event
                if hasattr(thread, "stop_event") and isinstance(thread.stop_event, threading.Event):
                    self.debug_print(f"Setting stop event for thread: {thread.name}")
                    thread.stop_event.set()

                try:
                    thread.join(timeout=5)
                    if thread.is_alive():
                        self.debug_print(f"Thread did not terminate: {thread.name}")
                        # Forcibly terminate as a last resort
                        self.debug_print(f"Forcing termination of thread: {thread.name}")
                        self.terminate_thread(thread)
                except Exception as e:
                    self.debug_print(f"Error while waiting for thread {thread.name}: {e}")

        # Stop the asyncio event loop
        if hasattr(self, 'loop') and self.loop.is_running():
            self.debug_print("Stopping asyncio event loop.")
            self.loop.call_soon_threadsafe(self.loop.stop)

        # Handle the loop_thread if it exists
        if hasattr(self, 'loop_thread') and self.loop_thread and self.loop_thread.is_alive():
            self.debug_print(f"Waiting for loop thread: {self.loop_thread.name}")
            self.loop_thread.join(timeout=5)
            if self.loop_thread.is_alive():
                self.debug_print(f"Loop thread did not terminate: {self.loop_thread.name}")
        else:
            self.debug_print("No loop thread to terminate.")

        # Call the base class shutdown method
        super().shutdown(wait)

        # Log remaining active threads after shutdown
        active_threads_after = threading.enumerate()
        self.debug_print(f"Active threads after shutdown: {[thread.name for thread in active_threads_after]}")
        self.debug_print("ThreadPoolExecutor shutdown complete.")

    def terminate_thread(self, thread):
        """Forcefully terminate a thread."""
        if not thread.is_alive():
            return
        tid = ctypes.c_long(thread.ident)
        exc = ctypes.py_object(SystemExit)
        res = ctypes.pythonapi.PyThreadState_SetAsyncExc(tid, exc)
        if res == 0:
            raise ValueError("Invalid thread ID")
        elif res > 1:
            ctypes.pythonapi.PyThreadState_SetAsyncExc(tid, None)
            raise SystemError("PyThreadState_SetAsyncExc failed")

    def debug_print(self, message):
        """Print debug messages if debugging is enabled."""
        if self.debug:
            try:
                frame = inspect.currentframe().f_back
                line_number = frame.f_lineno
                function_name = frame.f_code.co_name
                frame2 = inspect.currentframe().f_back.f_back
                ln2 = frame2.f_lineno
                fn2 = frame2.f_code.co_name
                print(f"[DEBUG] [{self.name}] fname:{function_name} ln:{line_number}: MSG:{message} Debug Info from: {ln2}:{fn2}")
            except Exception as e:
                print(f"[DEBUG ERROR] Failed to print debug information: {e}")

    def count_frames_to_top(self, frame):
        count = 0
        # Traverse back through the call stack
        while frame:
            count += 1
            frame = frame.f_back  # Move to the previous frame
        return count

    def get_frame_by_depth(self, depth, frame):
        """Access a specific frame by depth."""
        for _ in range(depth - 1):
            if frame is None:
                break  # In case the depth exceeds the actual stack size
            frame = frame.f_back
        return frame


class AsyncioThreadManager:
    """Utility to create and manage custom event loops with named threads."""
    def __init__(self, thread_name_prefix="NamedAsyncioThread", max_workers=10, debug=False):
        self.thread_name_prefix = thread_name_prefix
        self.max_workers = max_workers
        self.loop = None
        self.loop_thread = None
        self.debug = True
        self.name = "AsyncioThreadManager"

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
        self.debug_print(f"{self.thread_name_prefix}: Event loop started in thread {self.loop_thread.name}")  # Debug print

    def _run_event_loop(self):
        """Run the event loop."""
        asyncio.set_event_loop(self.loop)
        self.debug_print(f"{self.thread_name_prefix}: Running event loop.")  # Debug print
        self.loop.run_forever()

    def run_until_complete(self, coro):
        """Run a coroutine until completion using the managed event loop."""
        if not self.loop:
            self.create_event_loop()
        asyncio.set_event_loop(self.loop)
        return self.loop.run_until_complete(coro)

    def shutdown(self):
        """Gracefully stop the event loop."""
        self.debug_print("Stopping AsyncioThreadManager...")
        self.debug_print(f"Threads before shutdown: {list(threading.enumerate())}")

        # Stop the event loop safely
        if self.loop.is_running():
            self.debug_print("Stopping the asyncio event loop...")
            self.loop.call_soon_threadsafe(self.loop.stop)

        # Wait for the loop thread to finish
        if self.loop_thread and self.loop_thread.is_alive():
            self.debug_print("Waiting for loop thread to finish...")
            self.loop_thread.join()

        # Handle pending tasks
        try:
            pending_tasks = asyncio.all_tasks(self.loop)
            if pending_tasks:
                self.debug_print(f"Found {len(pending_tasks)} pending tasks. Cancelling them...")

                def cancel_tasks():
                    for task in pending_tasks:
                        if not task.done():
                            self.debug_print(f"Cancelling task: {task}")
                            task.cancel()

                    # Await pending tasks
                    async def wait_for_tasks():
                        await asyncio.gather(*pending_tasks, return_exceptions=True)
                        self.debug_print("All tasks completed.")

                    asyncio.create_task(wait_for_tasks())

                self.loop.call_soon_threadsafe(cancel_tasks)
        except Exception as e:
            self.debug_print(f"Error while completing tasks: {e}")

        # Clean up asynchronous generators
        try:
            async def cleanup_generators():
                await self._cleanup_asyncgens()
                self.debug_print("Async generators cleaned up.")

            self.loop.call_soon_threadsafe(asyncio.create_task, cleanup_generators())
        except Exception as e:
            self.debug_print(f"Error while cleaning up async generators: {e}")

        # Shut down the default executor, if applicable
        if isinstance(self.loop._default_executor, ThreadPoolExecutor):
            self.debug_print("Shutting down ThreadPoolExecutor...")
            self.loop._default_executor.shutdown(wait=True)

        # Close the event loop
        if not self.loop.is_closed():
            self.loop.close()
            self.debug_print("Event loop closed.")

        # Final cleanup
        self.loop = None
        self.debug_print("AsyncioThreadManager stopped.")
        self.debug_print(f"Threads after shutdown: {list(threading.enumerate())}")


    async def _cleanup_asyncgens(self):
        """Helper coroutine to shutdown asynchronous generators."""
        try:
            await self.loop.shutdown_asyncgens()
        except Exception as e:
            self.debug_print(f"Error shutting down async generators: {e}")

    def debug_print(self, message):
        """
        Prints debug messages to the console if self.debug is enabled.
        Dynamically retrieves the calling line number and function name.
        
        Args:
            message (str): The debug message to print.
        """
        if self.debug:
            try:
                # Get the frame of the caller
                frame = inspect.currentframe().f_back
                line_number = frame.f_lineno
                function_name = frame.f_code.co_name
                frame2 = inspect.currentframe().f_back.f_back
                ln2 = frame2.f_lineno
                fn2 = frame2.f_code.co_name
                
                print(f"[DEBUG] [{self.name}] {function_name} ln{line_number}: {message} from: {ln2}:{fn2}")
            except Exception as e:
                print(f"[DEBUG ERROR] Failed to print debug information: {e}")

