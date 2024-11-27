import argparse
import threading
import json
import socket
import asyncio
import signal
import inspect
from core.cip_emulator import CIPEmulator
from utils.config_loader import load_config
from utils.logger import create_threaded_logger
from utils.asynciothreadmanager import AsyncioThreadManager
import atexit
import cProfile
import sys
import traceback

# Set a name for the main thread
threading.current_thread().name = "MainCIPThread"

CONFIG_PATH = "config.json"  # Path to the configuration file


@atexit.register
def on_exit():
    """Clean-up function triggered at exit."""
    print("Exiting the emulator...")


def display_config(app_config, consumer_config, producer_config):
    """
    Prints the current configuration sections in a readable format.
    """
    print("Application Configuration:")
    print(json.dumps(app_config, indent=4))
    print("\nConsumer Configuration:")
    print(json.dumps(consumer_config, indent=4))
    print("\nProducer Configurations:")
    print(json.dumps(producer_config, indent=4))


async def run_emulator(args, config, hostname, stop_event, threadmanager):
    app_config, consumer_config, producers_config = (
        config["app"],
        config["consumer"],
        config["producers"],
    )

    emulator_logger = create_threaded_logger(threadmanager, name=f"{hostname}_emulator")
    emulator_logger.info(f"Starting CIP Emulator in thread: {threading.current_thread().name}.")

    try:
        emulator = CIPEmulator(
            app_config,
            consumer_config,
            producers_config,
            gui_mode=args.no_gui,
            quiet=args.quiet,
            threadmanager= threadmanager
        )
        emulator_logger.info("CIP Emulator initialized successfully.")
    except Exception as e:
        emulator_logger.error(f"Failed to initialize CIP Emulator: {e}")
        raise

    try:
        if args.server_only:
            emulator_logger.info("Starting server only.")
            await emulator.start()
        elif args.client_only:
            emulator_logger.info("Starting clients only.")
            await emulator.start_all_clients()
        elif args.both:
            emulator_logger.info("Starting both server and clients.")
            await emulator.start()
            await asyncio.sleep(1)  # Ensure the server is ready
            await emulator.start_all_clients()

        emulator_logger.info("Press Ctrl+C to stop the emulator.")
        # Keep the emulator running until stop_event is set
        await stop_event.wait()

    except asyncio.CancelledError:
        emulator_logger.warning("Cancellation received. Cleaning up...")
    except KeyboardInterrupt:
        emulator_logger.info("Keyboard interrupt received. Stopping emulator...")
    finally:
        await shutdown_emulator(emulator, emulator_logger, args)

async def shutdown_emulator(emulator, logger, args):
    """
    Gracefully stop emulator components and shut down the logger.
    """
    try:
        logger.info("Shutting down server...")
        await emulator.stop()
    except Exception as e:
        logger.error(f"Error during shutdown: {e}")
    finally:
        logger.info("Stopping logger as the last step.")
        await logger.stop()
        await asyncio.sleep(1)  # Ensure all log messages are flushed

async def main(debug=False):
    """
    Main entry point for the CIP Emulator. Parses arguments, loads configurations, and starts the emulator.
    """
    # Load configuration
    config = load_config(CONFIG_PATH)
    app_config, consumer_config, producers_config = (
        config.get("app", {}),
        config.get("consumer", {}),
        config.get("producers", {}),
    )
    hostname = socket.gethostname()
    debug = debug

    # Parse command-line arguments
    parser = argparse.ArgumentParser(
        description="CIP Emulator - Run in GUI or headless mode with server, client, or both."
    )
    parser.add_argument("--no-gui", action="store_true", help="Run the emulator in headless mode without GUI.")
    parser.add_argument("--server-only", action="store_true", help="Run only the server.")
    parser.add_argument("--client-only", action="store_true", help="Run only the clients.")
    parser.add_argument("--both", action="store_true", help="Run both server and clients.")
    parser.add_argument("--show-config", action="store_true", help="Display the current configuration and exit.")
    parser.add_argument("--quiet", action="store_true", help="Suppress packet-sending logs from clients.")
    args = parser.parse_args()

    # Handle --show-config argument
    if args.show_config:
        display_config(app_config, consumer_config, producers_config)
        return

    # Create and start the thread manager
    threadmanager = AsyncioThreadManager(thread_name_prefix="EmulatorManager", max_workers=5)
    threadmanager.start_event_loop()

    # Create a stop event
    stop_event = asyncio.Event()

    # Signal handler for graceful shutdown
    def signal_handler(sig, frame):
        debug_print(f"Signal received ({sig}). Shutting down emulator...", debug=debug)
        stop_event.set()

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    try:
        # Define the task to run
        async def run_emulator_task():
            await run_emulator(args, config, hostname, stop_event, threadmanager)

        # Schedule the monitoring task
        asyncio.run_coroutine_threadsafe(
            monitor_threads_and_tasks(interval=30, debug=debug),  # Run monitoring every 30 seconds
            threadmanager.loop
        )

        # Schedule the main emulator task
        task = asyncio.run_coroutine_threadsafe(
            run_emulator_task(),
            threadmanager.loop
        )

        # Wait for the task to complete
        task.result()  # This blocks until the task is done or raises an exception

    except asyncio.CancelledError:
        debug_print("Tasks cancelled. Cleaning up...", debug=debug)
    except Exception as e:
        debug_print(f"Exception occurred: {e}", debug=debug)
    finally:
        debug_print("Stopping emulator task...", debug=debug)
        if task and not task.done():
            task.cancel()  # Cancel the task
            try:
                task.result()  # Retrieve the result or raise exception (handles CancelledError)
                debug_print("Emulator task stopped successfully.", debug=debug)
            except asyncio.CancelledError:
                debug_print("Emulator task was cancelled.", debug=debug)
            except Exception as e:
                debug_print(f"Exception while stopping emulator task: {e}", debug=debug)
        debug_print("Stopping event loop and cleaning up tasks...", debug=debug)
        threadmanager.shutdown()
        debug_print(f"Active threads after shutdown: {[thread.name for thread in threading.enumerate()]}", debug=debug)
        debug_print("Event loop closed.", debug=debug)
        # Exit cleanly
        print("Exiting MainCIPThread.")
        sys.exit(0)

async def monitor_threads_and_tasks(interval=120, debug=False):
    """
    Periodically enumerates all threads and asyncio tasks, and displays the current stack trace
    for each thread. Logs or prints the details every `interval` seconds.
    """
    debug = debug
    while True:
        threads_info = []
        current_frames = sys._current_frames()  # Get stack frames for all threads
        for thread in threading.enumerate():
            thread_info = {
                "thread_name": thread.name,
                "thread_id": thread.ident,
                "tasks": [],
                "stack": None
            }
            if thread.ident in current_frames:
                frame = current_frames[thread.ident]
                thread_info["stack"] = traceback.extract_stack(frame)

            if hasattr(thread, "_loop") and thread._loop is not None:
                loop = thread._loop
                # Retrieve all asyncio tasks in the loop
                tasks = asyncio.all_tasks(loop=loop)
                thread_info["tasks"] = [{"task": str(task), "coro": str(task.get_coro())} for task in tasks]

            threads_info.append(thread_info)

        # Log or print thread and task details
        debug_print("Enumerating threads and tasks:", debug=debug)
        for info in threads_info:
            debug_print(f"Thread {info['thread_name']} (ID: {info['thread_id']}):", debug=debug)
            for task in info["tasks"]:
                debug_print(f"    Task: {task['task']}, Coroutine: {task['coro']}", debug=debug)
            if info["stack"]:
                debug_print(f"  Current Stack Trace for Thread {info['thread_name']}:", debug=debug)
                for line in info["stack"]:
                    debug_print(f"    {line}", debug=debug)

        await asyncio.sleep(interval)


def debug_print(message, debug=False):
    """
    Prints debug messages to the console if self.debug is enabled.
    Dynamically retrieves the calling line number and function name.
    
    Args:
        message (str): The debug message to print.
    """
    if debug:
        # Get the frame of the caller
        frame = inspect.currentframe().f_back
        line_number = frame.f_lineno
        function_name = frame.f_code.co_name
        frame2 = inspect.currentframe().f_back.f_back
        ln2 = frame2.f_lineno
        fn2 = frame2.f_code.co_name
        
        print(f"[DEBUG] [emulator_main] {function_name} ln{line_number}: {message} from: {ln2}:{fn2}")


if __name__ == "__main__":
    cProfile.run("asyncio.run(main(debug=True))", "profile_results.prof")
