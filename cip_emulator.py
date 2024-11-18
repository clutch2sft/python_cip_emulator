import argparse
import json
import socket
import asyncio
from core.cip_emulator import CIPEmulator
from utils.config_loader import load_config
from utils.logger import create_threaded_logger
import atexit
import cProfile

CONFIG_PATH = "config.json"  # Path to the configuration file


@atexit.register
def on_exit():
    """Clean-up function triggered at exit."""
    print("Main entry function: Exiting the emulator...")


async def run_emulator(args, config, hostname):
    """
    Main coroutine to run the emulator in either GUI or headless mode based on arguments.
    """
    app_config, consumer_config, producers_config = (
        config["app"],
        config["consumer"],
        config["producers"],
    )

    emulator_logger = create_threaded_logger(name=f"{hostname}_emulator")
    emulator_logger.info("Main entry function: Starting CIP Emulator.")

    # Initialize loggers
    server_logger = None
    client_loggers = None

    if args.server_only or args.both:
        server_logger = create_threaded_logger(name=f"{hostname}_server", use_ansi_colors=True)
        emulator_logger.info(f"Main entry function: Server logger initialized as {hostname}_server")

    if args.client_only or args.both:
        client_loggers = {
            tag: create_threaded_logger(name=f"{hostname}_client_{tag}", use_ansi_colors=True)
            for tag in producers_config
        }
        for client_logger_name in client_loggers.keys():
            emulator_logger.info(f"Main entry function: Initializing client logger for {client_logger_name}")

    # Initialize the emulator
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

    # Start components based on CLI arguments
    try:
        if args.server_only:
            emulator_logger.info("Main entry function: Starting server only.")
            await emulator.start_server()
        elif args.client_only:
            emulator_logger.info("Main entry function: Starting clients only.")
            await emulator.start_all_clients()
        elif args.both:
            emulator_logger.info("Main entry function: Starting both server and clients.")
            await emulator.start_server()
            await asyncio.sleep(1)  # Ensure the server is ready
            await emulator.start_all_clients()

        emulator_logger.info("Main entry function: Press Ctrl+C to stop the emulator.")
        while True:
            await asyncio.sleep(0.5)  # Keep the loop alive and responsive to interrupts

    except KeyboardInterrupt:
        emulator_logger.info("Main entry function: Keyboard interrupt received. Shutting down emulator...")
        if args.server_only or args.both:
            await emulator.stop_server()
        if args.client_only or args.both:
            await emulator.stop_all_clients()


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

    # Determine mode: GUI by default if no CLI flags are set
    if not any([args.no_gui, args.server_only, args.client_only, args.both]):
        from gui.cip_gui import CIPGUI

        emulator_logger = create_threaded_logger(name=f"{hostname}_emulator")
        emulator_logger.info("Main entry function: Starting CIP Emulator in GUI mode.")
        gui = CIPGUI(config_path=CONFIG_PATH, logger_app=emulator_logger)
        gui.run()
        return

    # Headless (CLI) mode
    asyncio.run(run_emulator(args, config, hostname))


if __name__ == "__main__":
    cProfile.run('main()', 'profile_results.prof')
