import time
import zmq
import threading

class TimeSyncServer:
    _instance = None  # Singleton instance

    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = super(TimeSyncServer, cls).__new__(cls)
        return cls._instance

    def __init__(self, ip_address='0.0.0.0', port=5555, logger_app=None, debug=False):
        if not hasattr(self, 'initialized'):
            self.ip_address = ip_address
            self.port = port
            self.logger = logger_app
            self.debug = debug
            self.context = zmq.Context()
            self.socket = self.context.socket(zmq.REP)  # Change to REP for request-response pattern
            self.socket.bind(f"tcp://{self.ip_address}:{self.port}")
            self.class_name = self.__class__.__name__
            self.running = False
            self.initialized = True  # Flag to ensure __init__ only runs once

    def start(self):
        """Start the server to respond to timestamp requests."""
        self.logger.info(f"{self.class_name}: Start called on timesync server address {self.ip_address}:{self.port}")
        self.running = True
        threading.Thread(target=self._run, daemon=True).start()

    def stop(self):
        """Stop the server."""
        self.logger.info(f"{self.class_name}: Stop called on timesync server address {self.ip_address}:{self.port}")
        self.running = False

    def _run(self):
        while self.running:
            # Wait for a request from the client
            message = self.socket.recv()  # Blocking until client sends a request
            if message == b"REQUEST_TIMESTAMP":
                # Send the current timestamp to the client
                timestamp_ns = time.time_ns()
                self.socket.send_pyobj(timestamp_ns)  # Send timestamp as response
                if self.debug:
                    self.logger.info(f"{self.class_name}: Server sent timestamp: {timestamp_ns}")

    def close(self):
        """Close the ZeroMQ socket and context."""
        self.socket.close()
        self.context.term()
        TimeSyncServer._instance = None  # Reset singleton instance
