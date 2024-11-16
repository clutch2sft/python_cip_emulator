# Import necessary modules
import argparse
import json
from core.cip_emulator import CIPEmulator
from utils.config_loader import load_config
from utils.logger import create_threaded_logger
import socket, time
import atexit
import time
import cProfile

CONFIG_PATH = "config.json"  # Path to the configuration file

@atexit.register
def on_exit():
    print("Main entry function: Exiting the emulator...")
    # Log to a file or your main logger if desired
    # self.logger_app.info("Exiting the emulator...")

def main():
    """
    Main entry function for the CIP Emulator. Parses arguments, configures logging,
    loads configurations, and starts the emulator in either GUI or headless mode based
    on command-line options.
    """
    # Load the configuration file and retrieve specific sections
    config = load_config(CONFIG_PATH)
    app_config = config.get("app", {})
    consumer_config = config.get("consumer", {})
    producers_config = config.get("producers", {})
    hostname = socket.gethostname()  # Retrieve the host name for logger identification

    # Set up argument parser with descriptions for each option
    parser = argparse.ArgumentParser(
        description="CIP Emulator - Run in GUI or headless mode with server, client, or both."
    )
    parser.add_argument("--no-gui", action="store_true", help="Run the emulator in headless mode without GUI.")
    parser.add_argument("--server-only", action="store_true", help="Run only the server.")
    parser.add_argument("--client-only", action="store_true", help="Run only the clients.")
    parser.add_argument("--both", action="store_true", help="Run both server and clients.")
    parser.add_argument("--show-config", action="store_true", help="Display the current configuration and exit.")
    parser.add_argument("--quiet", action="store_true", help="Suppress packet-sending logs from clients.")

    # Parse command-line arguments
    args = parser.parse_args()

    # If --show-config is specified, display the configuration and exit
    if args.show_config:
        display_config(app_config, consumer_config, producers_config)
        return

    # Determine mode: GUI by default if no CLI mode flags are set
    if not (args.no_gui or args.server_only or args.client_only or args.both):
        # Run GUI mode if no headless flags are set
        from gui.cip_gui import CIPGUI
        emulator_logger = create_threaded_logger(name=f"{hostname }_emulator")
        emulator_logger.info("Main entry function: Starting CIP Emulator in GUI mode.")
        gui = CIPGUI(config_path=CONFIG_PATH, logger_app=emulator_logger)
        gui.run()  # Start the GUI
        return

    # Headless mode (CLI)
    if args.no_gui or args.server_only or args.client_only or args.both:
        emulator_logger = create_threaded_logger(name=f"{hostname}_emulator")
        emulator_logger.info("Main entry function: Starting CIP Emulator in headless mode.")

        # Initialize server logger if the server is needed
        server_logger = None
        if args.server_only or args.both:
            server_logger = create_threaded_logger(name=f"{hostname}_server", use_ansi_colors=True)
            emulator_logger.info(f"Main entry function: Server logger initialized as {hostname}_server")

        # Initialize client loggers for each configured producer if clients are needed
        client_loggers = None
        if args.client_only or args.both:
            client_loggers = {}
            for tag in producers_config:
                client_logger_name = f"{hostname}_client_{tag}"
                emulator_logger.info(f"Main entry function: Initializing client logger for {client_logger_name}")
                client_loggers[tag] = create_threaded_logger(name=client_logger_name, use_ansi_colors=True)

        # Initialize CIPEmulator with the necessary loggers and configuration

        emulator = CIPEmulator(
            app_config,
            consumer_config,
            producers_config,
            logger_server=server_logger,
            logger_client=client_loggers,
            logger_app=emulator_logger,
            gui_mode=False,
            quiet=args.quiet
        )

        # Start server, clients, or both based on specified arguments
        try:
            if args.server_only:
                emulator_logger.info("Main entry function: Starting server only.")
                emulator.start_server()
            elif args.client_only:
                emulator_logger.info("Main entry function: Starting clients only.")
                emulator.start_all_clients()
            elif args.both:
                emulator_logger.info("Main entry function: Starting both server and clients.")
                emulator.start_server()
                time.sleep(1)  # Wait for server readiness
                emulator.start_all_clients()

            # Keep CLI running until interrupted
            emulator_logger.info("Main entry function: Press Ctrl+C to stop the emulator.")
            while True:
                time.sleep(0.5)  # Short sleep to reduce CPU usage if loop is empty
                    # Alternatively, add a condition to exit this loop if needed
                pass

        except KeyboardInterrupt:
            emulator_logger.info("Main entry function: Keyboard interrupt received. Shutting down emulator...")
            # Gracefully stop components if running
            if args.server_only or args.both:
                emulator.stop_server()
            if args.client_only or args.both:
                emulator.stop_all_clients()

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


if __name__ == "__main__":
    #main()
    cProfile.run('main()', 'profile_results.prof')