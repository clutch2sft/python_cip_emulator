import time
import grpc
from concurrent import futures
import threading
import timesync_pb2
import timesync_pb2_grpc


class TimeSyncService(timesync_pb2_grpc.TimeSyncServiceServicer):
    def __init__(self, logger_app=None, debug=False):
        self.logger = logger_app
        self.debug = debug

    def RequestTimestamp(self, request, context):
        """Handles client requests for server time."""
        timestamp_ns = time.time_ns()
        if self.debug:
            self.logger.info(f"TimeSyncService: Sending timestamp: {timestamp_ns}")
        return timesync_pb2.TimeResponse(timestamp_ns=timestamp_ns)

    def CheckHealth(self, request, context):
        """Simple health check endpoint."""
        return timesync_pb2.HealthCheckResponse(healthy=True)


class TimeSyncServer:
    _instance = None  # Singleton instance

    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = super(TimeSyncServer, cls).__new__(cls)
        return cls._instance

    def __init__(self, ip_address='0.0.0.0', port=5555, logger_app=None, debug=False):
        if not hasattr(self, 'initialized'):  # Initialize only once (singleton pattern)
            self.ip_address = ip_address
            self.port = port
            self.logger = logger_app
            self.debug = debug
            self.server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
            self.service = TimeSyncService(logger_app=self.logger, debug=self.debug)
            timesync_pb2_grpc.add_TimeSyncServiceServicer_to_server(self.service, self.server)
            self.server.add_insecure_port(f'{self.ip_address}:{self.port}')
            self.running = False
            self.initialized = True

    def start(self):
        """Starts the gRPC server to listen for incoming timestamp requests."""
        if not self.running:
            self.logger.info(f"TimeSyncServer: Starting on {self.ip_address}:{self.port}")
            self.running = True
            self.server.start()
            threading.Thread(target=self._monitor_server, daemon=True).start()
        else:
            self.logger.warning("TimeSyncServer: Server is already running.")

    def stop(self):
        """Stops the gRPC server."""
        if self.running:
            self.logger.info(f"TimeSyncServer: Stopping on {self.ip_address}:{self.port}")
            self.running = False
            self.server.stop(grace=5)
        else:
            self.logger.warning("TimeSyncServer: Server is already stopped.")

    def _monitor_server(self):
        """Monitor server activity and log if in debug mode."""
        while self.running:
            if self.debug:
                self.logger.info("TimeSyncServer: Running and ready to accept requests.")
            time.sleep(5)

    def close(self):
        """Closes the server and resets the singleton instance."""
        self.stop()
        self.logger.info("TimeSyncServer: Server has been closed.")
        TimeSyncServer._instance = None  # Reset the singleton instance

