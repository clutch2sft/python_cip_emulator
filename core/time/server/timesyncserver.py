import time
import grpc
from concurrent import futures
import threading
from core.time.server import timesync_pb2
from  core.time.server import timesync_pb2_grpc


class TimeSyncService(timesync_pb2_grpc.TimeSyncServiceServicer):
    def __init__(self, logger_app=None, debug=False):
        self.logger = logger_app
        self.debug = debug
        self.start_time = time.time()
        self.request_count = 0
        self.total_response_time = 0.0

    def RequestTimestamp(self, request, context):
        """Handles client requests for server time."""
        start_processing_time = time.time()
        self.request_count += 1

        timestamp_ns = time.time_ns()
        if self.debug:
            self.logger.info(f"TimeSyncService: Sending timestamp: {timestamp_ns}")

        # Update response time stats
        self.total_response_time += (time.time() - start_processing_time) * 1000  # Convert to ms

        return timesync_pb2.TimeResponse(timestamp_ns=timestamp_ns)

    def RequestTimestamp(self, request, context):
        """Handles client requests for server time."""
        timestamp_ns = time.time_ns()
        if self.debug:
            self.logger.info(f"TimeSyncService: Sending timestamp: {timestamp_ns}")
        return timesync_pb2.TimeResponse(timestamp_ns=timestamp_ns)

    def CheckHealth(self, request, context):
        """Returns server health and stats."""
        uptime_seconds = int(time.time() - self.start_time)
        avg_response_time_ms = (self.total_response_time / self.request_count) if self.request_count > 0 else 0.0

        if self.debug:
            self.logger.info(
                f"TimeSyncService: Health check - Uptime: {uptime_seconds}s, "
                f"Requests: {self.request_count}, Avg Response Time: {avg_response_time_ms:.3f}ms"
            )

        return timesync_pb2.HealthCheckResponse(
            healthy=True,
            uptime_seconds=uptime_seconds,
            request_count=self.request_count,
            avg_response_time_ms=avg_response_time_ms
        )

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
        while self.running and self.debug:
            self.logger.info("TimeSyncServer: Running and ready to accept requests.")
            time.sleep(5)

    def close(self):
        """Closes the server and resets the singleton instance."""
        self.stop()
        self.logger.info("TimeSyncServer: Server has been closed.")
        TimeSyncServer._instance = None  # Reset the singleton instance

