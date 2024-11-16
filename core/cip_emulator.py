from core.cip_server import CIPServer
from core.cip_client import CIPClient
from core.time.server.timesyncserver import TimeSyncServer
from core.time.client.timesyncclient import TimeSyncClient
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
        Clients only run if explicitly started using `start_all_clients()` or `start_gui_clients()`. This approach allows 
        for flexible activation of individual clients while maintaining object references.
        """
        self.app_config = app_config
        self.consumer_config = consumer_config
        self.producers_config = producers_config
        self.gui_mode = gui_mode
        self.quiet = quiet
        self.logger_app = logger_app
        self.filter_factor=self.consumer_config.get("latency_filter_factor") #its a bit in the wrong place but it also doesn't make sense to put it in multiple times under each client.
        self.class_name = self.__class__.__name__
        
        if logger_server is None:
            # Set up the server logger or placeholder null logger
            self.server_logger = self._create_logger_adapter(logger_server) if logger_server else self._null_logger
        if logger_client is None:
            self.logger_client = logger_client or {}
        
        # Initialize TimeSyncServer if in CLI mode
        self.tsync_server = None
        if not gui_mode:
            self._initialize_time_sync_server()
        
        # Initialize CIPServer if a server logger is provided
        self.server = CIPServer(self.server_logger, consumer_config) if logger_server else None

        # Initialize producers (clients) based on the provided configuration
        self.producers = []
        for client_tag, producer_config in producers_config.items():
            # Create a specific logger for each client
            producer_logger = self._create_client_logger(client_tag)
            producer = CIPClient(producer_logger, producer_config, tag=client_tag, quiet=self.quiet, logger_app=self.logger_app)
            self.producers.append(producer)

        # Placeholder for TimeSyncClient instance (created in start_all_clients or start_gui_clients)
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
        function based on whether GUI or CLI mode is used and respects the 'quiet' flag.
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
                    log_method(message, log_to_console_cancel=self.quiet)
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
            return lambda message, level="INFO": getattr(client_logger, level.lower(), client_logger.info)(
                message,
                log_to_console_cancel=self.quiet
            )
            #return lambda message, level="INFO": getattr(client_logger, level.lower(), client_logger.info)(f"[{client_tag}] {message}")

    def _initialize_time_sync_server(self):
        """Initializes the TimeSyncServer for time synchronization in CLI mode."""
        self.tsync_server = TimeSyncServer(logger_app=self.logger_app)
        self.tsync_server.start()

    def _initialize_time_sync_client(self):
        """Initializes the TimeSyncClient and waits for stability in both CLI and GUI modes."""
        stability_reached = False
        if not self.tsync_client:
            try:
                self.tsync_client = TimeSyncClient(
                    server_ip=self.consumer_config.get("server_ip"),
                    logger_app=self.logger_app,
                    server_port=self.consumer_config.get("time_sync_port", 5555),
                    txrate=0.1,
                    filter_factor=self.filter_factor
                )
                self.logger_app.info(f"{self.class_name}: Attempting to start Time Sync Client.")
                
                if not self.tsync_client.start():
                    self.logger_app.error(f"{self.class_name}: Server unreachable, aborting client startup.")
                    return False

                # Wait until stability is detected, with timeout
                stability_reached = self._wait_for_stability()
            except Exception as e:
                self.logger_app.error(f"{self.class_name}: Failed to start TimeSyncClient: {e}")
        return stability_reached

    def _wait_for_stability(self):
        """Waits for TimeSyncClient to reach stability with a timeout."""
        start_time = time.time()
        max_wait_time = 10
        last_log_time = start_time
        
        while not self.tsync_client.is_stable():
            current_time = time.time()
            if current_time - start_time > max_wait_time:
                self.logger_app.warning(f"{self.class_name}: Time sync stability check timed out.")
                return False

            if current_time - last_log_time >= 1:
                self.logger_app.info(f"{self.class_name}: Waiting for time sync stability...")
                last_log_time = current_time

            time.sleep(0.1)
        self.logger_app.info(f"{self.class_name}: Stability detected.")
        return True

    def start_gui_clients(self):
        """Starts clients in GUI mode, including initializing TimeSyncServer and TimeSyncClient."""
        
        stability_reached = self._initialize_time_sync_client()
        if stability_reached:
            for producer in self.producers:
                producer.start()  # Start each producer client in GUI mode

    def start_all_clients(self):
        """Starts all configured CIPClient instances (producers) and initializes TimeSyncClient in CLI mode."""
        stability_reached = self._initialize_time_sync_client()
        if stability_reached:
            for producer in self.producers:
                producer.start()  # Start each producer client in CLI mode

    def stop_all_clients(self):
        """Stops all configured CIPClient instances (producers) and shuts down TimeSyncClient."""
        for producer in self.producers:
            producer.stop()

        if self.tsync_client:
            self.tsync_client.stop()
            self.tsync_client = None

    def stop_gui_clients(self):
        """Stops clients in GUI mode."""
        self.stop_all_clients()  # Reuse the same stop method for simplicity

    def start_server(self):
        """Starts the CIPServer instance if it has been initialized."""
        # Start TimeSyncServer in GUI mode if it hasn't been started yet
        if self.gui_mode and not self.tsync_server:
            self._initialize_time_sync_server()

        if self.server:
            try:
                self.server.start()
                self.logger_app.info(f"{self.class_name}: Server started successfully.")
            except Exception as e:
                self.logger_app.error(f"{self.class_name}: Failed to start server: {e}")

    def stop_server(self):
        """Stops the CIPServer instance and TimeSyncServer if they have been initialized."""
        if self.tsync_server:
            try:
                self.tsync_server.stop()
                self.logger_app.info(f"{self.class_name}: TimeSyncServer stopped.")
            except Exception as e:
                self.logger_app.error(f"{self.class_name}: Failed to stop TimeSyncServer: {e}")

        if self.server:
            try:
                self.server.stop()
                self.logger_app.info(f"{self.class_name}: Server stopped successfully.")
            except Exception as e:
                self.logger_app.error(f"{self.class_name}: Failed to stop server: {e}")
