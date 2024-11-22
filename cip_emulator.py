import argparse
import threading
import json
import socket
import asyncio
import signal
from core.cip_emulator import CIPEmulator  # Ensure this module exists and is correct
from utils.config_loader import load_config
from utils.logger import create_threaded_logger
from utils.asynciothreadmanager import AsyncioThreadManager
import atexit
import cProfile

# Set a name for the main thread
threading.current_thread().name = "MainCIPThread"


CONFIG_PATH = "config.json"  # Path to the configuration file


@atexit.register
def on_exit():
    """Clean-up function triggered at exit."""
    print("Main entry function: Exiting the emulator...")

async def monitor_emulator_state(interval: int = 10, emulator_logger=None ):
    while True:
        emulator_logger.info(f"Active threads: {[t.name for t in threading.enumerate()]}")
        emulator_logger.info(f"Active tasks: {[task.get_name() for task in asyncio.all_tasks()]}")
        await asyncio.sleep(interval)



async def run_emulator(args, config, hostname, stop_event):
    """
    Main coroutine to run the emulator in either GUI or headless mode based on arguments.
    """
    app_config, consumer_config, producers_config = (
        config["app"],
        config["consumer"],
        config["producers"],
    )

    emulator_logger = create_threaded_logger(name=f"{hostname}_emulator")
    emulator_logger.info(f"Main entry function: Starting CIP Emulator in thread: {threading.current_thread().name}.")

    # Log task details
    current_task = asyncio.current_task()
    emulator_logger.info(f"Running in task '{current_task.get_name()}' (ID: {id(current_task)}).")

    # Initialize loggers
    server_logger = None
    client_loggers = None

    if args.server_only or args.both:
        server_logger = create_threaded_logger(name=f"{hostname}_server", use_ansi_colors=True)
        emulator_logger.info(f"Server logger initialized as {hostname}_server in thread: {threading.current_thread().name}.")

    if args.client_only or args.both:
        client_loggers = {
            tag: create_threaded_logger(name=f"{hostname}_client_{tag}", use_ansi_colors=True)
            for tag in producers_config
        }
        for client_logger_name in client_loggers.keys():
            emulator_logger.info(f"Initializing client logger '{client_logger_name}' in thread: {threading.current_thread().name}.")

    try:
        emulator = CIPEmulator(
            app_config,
            consumer_config,
            producers_config,
            logger_server=server_logger,
            logger_client=client_loggers,
            logger_app=emulator_logger,
            gui_mode=False,
            quiet=args.quiet,
        )
        emulator_logger.info("CIP Emulator initialized successfully.")
    except Exception as e:
        emulator_logger.error(f"Failed to initialize CIP Emulator: {e}")
        raise

    try:
        # Start components based on CLI arguments
        if args.server_only:
            emulator_logger.info("Starting server only.")
            await emulator.start_server()
        elif args.client_only:
            emulator_logger.info("Starting clients only.")
            await emulator.start_all_clients()
        elif args.both:
            emulator_logger.info("Starting both server and clients.")
            await emulator.start_server()
            await asyncio.sleep(1)  # Ensure the server is ready
            await emulator.start_all_clients()

        emulator_logger.info("Press Ctrl+C to stop the emulator.")

        # Monitor threads/tasks during runtime
        asyncio.create_task(monitor_emulator_state(interval=10, emulator_logger=emulator_logger))

        # Wait until the stop_event is set
        await stop_event.wait()

    except asyncio.CancelledError:
        emulator_logger.warning("Cancellation received. Cleaning up...")
    finally:
        await shutdown_emulator(emulator, emulator_logger, args)


async def shutdown_emulator(emulator, logger, args):
    """
    Gracefully stop emulator components and shut down the logger.
    """
    try:
        if args.server_only or args.both:
            logger.info("Main entry function: Shutting down server.")
            try:
                await emulator.stop_server()
            except asyncio.CancelledError:
                logger.warning("Server shutdown interrupted by cancellation.")
        if args.client_only or args.both:
            logger.info("Main entry function: Shutting down clients.")
            try:
                await emulator.stop_all_clients()
            except asyncio.CancelledError:
                logger.warning("Client shutdown interrupted by cancellation.")
    except Exception as e:
        logger.error(f"Main entry function: Error during shutdown: {e}")
    finally:
        # Stop the logger as the final step
        logger.info("Main entry function: Stopping logger as last step.")
        try:
            await emulator.stop_logger()
        except asyncio.CancelledError:
            logger.warning("Logger shutdown interrupted by cancellation.")


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


def main():
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

    # Manual event loop for graceful shutdown
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    # Create a stop event
    stop_event = asyncio.Event()

    def signal_handler(sig, frame):
        current_thread = threading.current_thread().name
        print(f"Signal received ({sig}) in thread {current_thread}. Cancelling tasks...")
        stop_event.set()
        for task in asyncio.all_tasks(loop):
            task.cancel()
        loop.call_soon_threadsafe(loop.stop)

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    try:
        loop.run_until_complete(run_emulator(args, config, hostname, stop_event))
    except asyncio.CancelledError:
        print("Main function: Tasks cancelled. Cleaning up...")
    finally:
        print("Main function: Stopping event loop and cleaning up tasks...")
        remaining_tasks = asyncio.all_tasks(loop)
        for task in remaining_tasks:
            task.cancel()
        loop.run_until_complete(asyncio.gather(*remaining_tasks, return_exceptions=True))
        loop.run_until_complete(loop.shutdown_asyncgens())
        loop.close()
        print("Event loop closed.")

if __name__ == "__main__":
    cProfile.run("main()", "profile_results.prof")
