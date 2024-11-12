from core.cip_server import CIPServer
from core.cip_client import CIPClient

class CIPEmulator:
    def __init__(self, app_config, consumer_config, producers_config, logger_server=None, logger_client=None, gui_mode=False, quiet = False):
        self.app_config = app_config
        self.consumer_config = consumer_config
        self.producers_config = producers_config
        self.gui_mode = gui_mode
        self.quiet = quiet

        # Conditionally initialize server and client loggers
        self.server_logger = self._create_logger_adapter(logger_server) if logger_server else self._null_logger
        self.logger_client = logger_client or {}

        # Initialize the consumer (server) if a server logger is provided
        self.server = CIPServer(self.server_logger, consumer_config) if logger_server else None

        # Initialize multiple producers (clients) based on provided configurations
        self.producers = []
        for client_tag, producer_config in producers_config.items():
            # Use _create_client_logger to provide a logger specifically for this client
            producer_logger = self._create_client_logger(client_tag)
            producer = CIPClient(producer_logger, producer_config, tag=client_tag, quiet=self.quiet)
            self.producers.append(producer)

    def _null_logger(self, message, level="INFO"):
        """A placeholder logger that does nothing."""
        pass

    def _create_logger_adapter(self, logger):
        """Creates a generic logger adapter based on GUI or CLI mode."""
        if self.gui_mode:
            # GUI mode: logger is a callable function for GUI queueing.
            def log(message, level="INFO"):
                logger(message, level=level)
        else:
            # CLI mode: logger is a full Logger instance.
            def log(message, level="INFO"):
                if logger:
                    log_method = getattr(logger, level.lower(), logger.info)
                    log_method(message)
        return log

    def _create_client_logger(self, client_tag):
        """Creates a specific logger for each client, with handling for GUI or CLI mode."""
        if self.gui_mode:
            # GUI mode: Use log_client_message with the specific client_tag
            return lambda message, level="INFO": self.logger_client(message, client_tag=client_tag, level=level)
        else:
            # CLI mode: Look up the logger for the specific client in the dictionary
            client_logger = self.logger_client.get(client_tag)
            if client_logger is None:
                # Use a placeholder logger if no client logger exists
                return self._null_logger

            # Return a logger that includes the client tag in the message
            return lambda message, level="INFO": getattr(client_logger, level.lower(), client_logger.info)(f"[{client_tag}] {message}")

    def start_server(self):
        """Start the server if it is initialized."""
        if self.server:
            self.server.start()

    def stop_server(self):
        """Stop the server if it is initialized."""
        if self.server:
            self.server.stop()

    def start_all_clients(self):
        """Start all configured producer clients."""
        for producer in self.producers:
            producer.start()

    def stop_all_clients(self):
        """Stop all configured producer clients."""
        for producer in self.producers:
            producer.stop()
