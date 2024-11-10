import argparse
import json
from core.cip_emulator import CIPEmulator
from utils.config_loader import load_config
from utils.logger import create_logger
import socket, time

CONFIG_PATH = "config.json"  # Path to the configuration file

def main():
    # Load the configuration
    config = load_config(CONFIG_PATH)
    app_config = config.get("app", {})
    consumer_config = config.get("consumer", {})
    producers_config = config.get("producers", {})
    hostname = socket.gethostname()
    # Set up argument parser with detailed help messages
    parser = argparse.ArgumentParser(
        description="CIP Emulator - Choose to run in GUI or headless mode with options for running server, client, or both."
    )
    parser.add_argument(
        "--no-gui", 
        action="store_true", 
        help="Run the emulator in headless mode without a GUI. Defaults to starting both server and client unless specified otherwise."
    )
    parser.add_argument(
        "--server-only", 
        action="store_true", 
        help="Run only the server in headless mode. Requires --no-gui if running without a GUI."
    )
    parser.add_argument(
        "--client-only", 
        action="store_true", 
        help="Run only the clients in headless mode. Requires --no-gui if running without a GUI."
    )
    parser.add_argument(
        "--both", 
        action="store_true", 
        help="Run both server and clients in headless mode. Requires --no-gui if running without a GUI."
    )
    parser.add_argument(
        "--show-config", 
        action="store_true", 
        help="Display the current configuration and exit without starting the emulator."
    )
    
    args = parser.parse_args()

    # Show the configuration and exit if --show-config is specified
    if args.show_config:
        display_config(app_config, consumer_config, producers_config)
        return

    # Check if any CLI mode flag is set; if not, default to GUI mode
    if not (args.no_gui or args.server_only or args.client_only or args.both):
        # Run GUI mode by default if no CLI mode flags are specified
        from gui.cip_gui import CIPGUI
        emulator_logger = create_logger(f"{hostname }_emulator")  # General logger for emulator lifecycle events
        emulator_logger.info("Starting CIP Emulator in GUI mode.")
        gui = CIPGUI(config_path=CONFIG_PATH)
        gui.run()
        return

    # Headless mode (CLI)
    if args.no_gui or args.server_only or args.client_only or args.both:
        emulator_logger = create_logger(f"{hostname}_emulator")
        emulator_logger.info("Starting CIP Emulator in headless mode.")

        # Initialize server logger only if the server is needed
        server_logger = None
        if args.server_only or args.both:
            server_logger = create_logger(f"{hostname}_server", use_ansi_colors=True)
            emulator_logger.info(f"Server logger initialized as {hostname}_server")

        # Initialize client loggers only if clients are needed
        client_loggers = None
        if args.client_only or args.both:
            client_loggers = {}
            for tag in producers_config:
                client_logger_name = f"{hostname}_client_{tag}"
                emulator_logger.info(f"Initializing client logger for {client_logger_name}")
                client_loggers[tag] = create_logger(client_logger_name, use_ansi_colors=True)

        # Initialize CIPEmulator with the required loggers
        emulator = CIPEmulator(
            app_config,
            consumer_config,
            producers_config,
            logger_server=server_logger,
            logger_client=client_loggers,
            gui_mode=False
        )

        # Start components based on arguments, with server-client coordination
        try:
            if args.server_only:
                emulator_logger.info("Starting server only.")
                emulator.start_server()
            elif args.client_only:
                emulator_logger.info("Starting clients only.")
                emulator.start_all_clients()
            elif args.both:
                emulator_logger.info("Starting both server and clients.")
                emulator.start_server()
                time.sleep(1)  # Ensure server readiness before clients start
                emulator.start_all_clients()

            emulator_logger.info("Press Ctrl+C to stop the emulator.")
            while True:
                pass  # Keep CLI running until interrupted

        except KeyboardInterrupt:
            emulator_logger.info("Keyboard interrupt received. Shutting down emulator...")
            # Stop components if running
            if args.server_only or args.both:
                emulator.stop_server()
            if args.client_only or args.both:
                emulator.stop_all_clients()

def display_config(app_config, consumer_config, producer_config):
    """Print the current configuration sections in a readable format."""
    print("Application Configuration:")
    print(json.dumps(app_config, indent=4))
    print("\nConsumer Configuration:")
    print(json.dumps(consumer_config, indent=4))
    print("\nProducer Configurations:")
    print(json.dumps(producer_config, indent=4))


if __name__ == "__main__":
    main()
