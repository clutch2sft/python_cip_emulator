from core.cip_server import CIPServer
from core.cip_client import CIPClient
from core.time.timesyncserver import TimeSyncServer
from core.time.timesyncclient import TimeSyncClient
import time
import atexit

class CIPEmulator:
    def __init__(self, app_config, consumer_config, producers_config, logger_server=None, logger_client=None, logger_app=None, gui_mode=False, quiet=False):
        """
        CIPEmulator initializes and controls the server and client components based on provided configurations.
        
        - **Server**: Created only if `logger_server` is provided. If `logger_server` is `None`, 
        the server is not instantiated.
        
        - **Clients**: Instantiated for each configuration in `producers_config` regardless of 
        logger availability. If no logger is provided for a client, a `_null_logger` is assigned.
        Clients only run if explicitly started using `start_all_clients()`. This approach allows 
        for flexible activation of individual clients while maintaining object references.
        """
        self.app_config = app_config
        self.consumer_config = consumer_config
        self.producers_config = producers_config
        self.gui_mode = gui_mode
        self.quiet = quiet
        self.logger_app = logger_app
        self.class_name = self.__class__.__name__
        # Set up the server logger or placeholder null logger
        self.server_logger = self._create_logger_adapter(logger_server) if logger_server else self._null_logger
        self.logger_client = logger_client or {}

        # Initialize TimeSyncServer for time synchronization support if a server logger is provided
        self.tsync_server = TimeSyncServer(logger_app=self.logger_app) if logger_server else None
        if self.tsync_server is not None:
            self.tsync_server.start()
        # Initialize CIPServer if a server logger is provided
        self.server = CIPServer(self.server_logger, consumer_config) if logger_server else None

        # Initialize producers (clients) based on the provided configuration
        self.producers = []
        for client_tag, producer_config in producers_config.items():
            # Create a specific logger for each client
            producer_logger = self._create_client_logger(client_tag)
            producer = CIPClient(producer_logger, producer_config, tag=client_tag, quiet=self.quiet, logger_app=self.logger_app)
            self.producers.append(producer)

        # Placeholder for TimeSyncClient instance (created in start_all_clients)
        self.tsync_client = None
    
    @atexit.register
    def on_exit():
        print(f"CIP Emulator: Exiting the emulator...")
        # Log to a file or your main logger if desired
        # self.logger_app.info("Exiting the emulator...")
    
    def _null_logger(self, message, level="INFO"):
        """A placeholder logger that does nothing, used when no logger is configured."""
        pass

    def _create_logger_adapter(self, logger):
        """
        Wraps a logger to allow mode-specific (GUI/CLI) logging. Adjusts the logging
        function based on whether GUI or CLI mode is used.
        """
        if self.gui_mode:
            # GUI mode: Use a callable for GUI queueing
            def log(message, level="INFO"):
                logger(message, level=level)
        else:
            # CLI mode: Use full Logger instance with log level handling
            def log(message, level="INFO"):
                if logger:
                    log_method = getattr(logger, level.lower(), logger.info)
                    log_method(message)
        return log

    def _create_client_logger(self, client_tag):
        """
        Creates a logger specific to each client, appending the client tag to messages.
        Returns a null logger if no logger is available for a client in CLI mode.
        """
        if self.gui_mode:
            # GUI mode: Use log_client_message with client tag
            return lambda message, level="INFO": self.logger_client(message, client_tag=client_tag, level=level)
        else:
            # CLI mode: Retrieve specific client logger or use a placeholder
            client_logger = self.logger_client.get(client_tag)
            if client_logger is None:
                return self._null_logger

            return lambda message, level="INFO": getattr(client_logger, level.lower(), client_logger.info)(f"[{client_tag}] {message}")

    def start_server(self):
        """Starts the CIPServer instance if it has been initialized."""
        if self.server:
            self.server.start()

    def stop_server(self):
        self.tsync_server.stop()
        """Stops the CIPServer instance if it has been initialized."""
        if self.server:
            self.server.stop()

    def start_all_clients(self):
        """Starts all configured CIPClient instances (producers) and initializes TimeSyncClient."""
        # Initialize TimeSyncClient singleton if not already running
        stability_reached = False
        if not self.tsync_client:
            try:
                self.logger_app.info(f"Logger type: {type(self.logger_app)}")
                #print(type(self.logger_app))  # Should output <class 'logging.Logger'>
                self.tsync_client = TimeSyncClient(
                    server_ip=self.consumer_config.get("server_ip"),
                    logger_app=self.logger_app,  # or assign a specific logger for timesync if desired
                    server_port=self.consumer_config.get("time_sync_port", 5555),
                    txrate=0.1  # Define the initial tx rate for measurements
                )
                self.logger_app.info(f"{self.class_name}: Attempting to start Time Sync Client.")
                # Start the TimeSyncClient; check if server is reachable
                if not self.tsync_client.start():  # start() now returns server_reachable
                    self.logger_app.error(f"{self.class_name}: Server unreachable, aborting client startup.")
                    return  # Exit if the server is unreachable

                # Wait until stability is detected, with optional timeout
                start_time = time.time()
                max_wait_time = 10  # Define a maximum wait time (e.g., 10 seconds)
                last_log_time = start_time  # Initialize last log time to the start time

                while not self.tsync_client.is_stable():
                    current_time = time.time()
                    # Check for timeout
                    if current_time - start_time > max_wait_time:
                        self.logger_app.warning(f"{self.class_name}: Time sync stability check timed out.")
                        break

                    # Log "Waiting for time sync stability..." only once per second
                    if current_time - last_log_time >= 1:
                        self.logger_app.info(f"{self.class_name}: Waiting for time sync stability...")
                        last_log_time = current_time

                    time.sleep(0.1)
                else:
                    # Stability was detected within the timeout period
                    stability_reached = True
                    self.logger_app.info(f"{self.class_name}: Stability detected, continuing with main application.")

            except Exception as e:
                self.logger_app.error(f"{self.class_name}: Failed to start TimeSyncClient: {e}")

            # Only start producers if stability was reached
            if stability_reached:
                for producer in self.producers:
                    producer.start()  # Start each producer client
            else:
                self.logger_app.error(f"{self.class_name}: Producers will not start due to time sync stability timeout.")
                self.tsync_client.stop()  # Stop TimeSyncClient if stability wasn't reached



    def stop_all_clients(self):
        """Stops all configured CIPClient instances (producers) and shuts down TimeSyncClient."""
        # Stop all producer clients
        for producer in self.producers:
            producer.stop()

        # Stop the TimeSyncClient singleton if running
        if self.tsync_client:
            self.tsync_client.stop()  # Assuming stop() is defined in TimeSyncClient
            self.tsync_client = None  # Clear the reference for future restarts