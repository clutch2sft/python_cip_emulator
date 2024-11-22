import time
import grpc.aio
from core.time.server import timesync_pb2
from core.time.server import timesync_pb2_grpc
import asyncio
import threading


class TimeSyncService(timesync_pb2_grpc.TimeSyncServiceServicer):
    def __init__(self, logger_app=None, debug=False):
        self.logger = logger_app
        self.debug = debug
        self.start_time = time.time()
        self.request_count = 0
        self.total_response_time = 0.0

    async def RequestTimestamp(self, request, context):
        """Handles client requests for server time."""
        start_processing_time = time.time()
        self.request_count += 1

        timestamp_ns = time.time_ns()
        if self.debug:
            self.logger.info(f"TimeSyncService: Sending timestamp: {timestamp_ns}")

        # Update response time stats
        self.total_response_time += (time.time() - start_processing_time) * 1000  # Convert to ms

        return timesync_pb2.TimeResponse(timestamp_ns=timestamp_ns)

    async def CheckHealth(self, request, context):
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
            avg_response_time_ms=avg_response_time_ms,
        )


class TimeSyncServer:
    _instance = None  # Singleton instance

    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = super(TimeSyncServer, cls).__new__(cls)
        return cls._instance

    def __init__(self, ip_address='0.0.0.0', port=5555, logger_app=None, debug=False):
        if not hasattr(self, "initialized"):  # Initialize only once (singleton pattern)
            self.ip_address = ip_address
            self.port = port
            self.logger = logger_app
            self.debug = debug
            self.stopped = False
            self.monitor_task = None  # Track the monitor task
            self.termination_task = None
            self.server = grpc.aio.server()
            self.service = TimeSyncService(logger_app=self.logger, debug=self.debug)
            timesync_pb2_grpc.add_TimeSyncServiceServicer_to_server(self.service, self.server)
            self.server.add_insecure_port(f"{self.ip_address}:{self.port}")
            self.initialized = True

    async def start(self):
        """Start the server and background tasks."""
        self.logger.info("Starting TimeSyncServer...")
        await self.server.start()
        self.logger.info("Server started.")
        self.termination_task = asyncio.create_task(self.server.wait_for_termination())
        if self.debug:
            self.monitor_task = asyncio.create_task(self._monitor_server())

    async def stop(self):
        """Stop the server and background tasks."""
        if self.stopped:
            return
        self.stopped = True
        self.logger.info("Stopping TimeSyncServer...")
        tasks = []
        if self.termination_task:
            self.termination_task.cancel()
            tasks.append(self.termination_task)
        if self.monitor_task:
            self.monitor_task.cancel()
            tasks.append(self.monitor_task)
        await asyncio.gather(*tasks, return_exceptions=True)
        await self.server.stop(grace=5)
        self.logger.info("TimeSyncServer stopped.")

    # async def start(self):
    #     """Starts the gRPC server to listen for incoming timestamp requests."""
    #     threading.current_thread().name = f"TimeSyncServer_MainThread"
    #     self.logger.info(f"TimeSyncServer: Starting on {self.ip_address}:{self.port}")
    #     await self.server.start()
    #     self.logger.info(f"TimeSyncServer: Server started on {self.ip_address}:{self.port}")

    #     termination_task = asyncio.create_task(self.server.wait_for_termination())
    #     self.monitor_task = asyncio.create_task(self._monitor_server()) if self.debug else None

    #     try:
    #         if self.monitor_task:
    #             await asyncio.gather(termination_task, self.monitor_task)
    #         else:
    #             await termination_task
    #     except asyncio.CancelledError:
    #         self.logger.warning("TimeSyncServer: Cancellation received, shutting down...")
    #         await self.stop()  # Ensure graceful shutdown
    #     finally:
    #         # Cancel tasks explicitly
    #         if termination_task:
    #             termination_task.cancel()
    #             try:
    #                 await termination_task
    #             except asyncio.CancelledError:
    #                 self.logger.info("TimeSyncServer: Termination task cancelled.")
    #         if self.monitor_task:
    #             self.monitor_task.cancel()
    #             try:
    #                 await self.monitor_task
    #             except asyncio.CancelledError:
    #                 self.logger.info("TimeSyncServer: Monitor task cancelled.")

    # async def stop(self):
    #     """Stops the gRPC server."""
    #     if self.stopped:
    #         self.logger.warning("TimeSyncServer: Stop called, but server is already stopped.")
    #         return

    #     self.stopped = True  # Mark as stopped to prevent duplicate calls

    #     try:
    #         self.logger.info(f"TimeSyncServer: Stopping on {self.ip_address}:{self.port}")

    #         # Cancel the monitor task if running
    #         if self.monitor_task:
    #             self.monitor_task.cancel()
    #             try:
    #                 await self.monitor_task
    #             except asyncio.CancelledError:
    #                 self.logger.info("TimeSyncServer: Monitor task cancelled during shutdown.")

    #         # Stop the gRPC server
    #         await self.server.stop(grace=5)
    #     except asyncio.CancelledError:
    #         self.logger.warning("TimeSyncServer: Cancellation received during shutdown.")
    #     except Exception as e:
    #         self.logger.error(f"TimeSyncServer: Error during shutdown: {e}")
    #     finally:
    #         self.logger.info("TimeSyncServer: Server has been stopped.")

    async def _monitor_server(self):
        """Monitor server activity and log if in debug mode."""
        threading.current_thread().name = f"TimeSyncServer_MonitorThread"
        try:
            while True:
                self.logger.info("TimeSyncServer: Running and ready to accept requests.")
                await asyncio.sleep(5)
        except asyncio.CancelledError:
            self.logger.info("TimeSyncServer: Monitoring task canceled.")

    def close(self):
        """Closes the server and resets the singleton instance."""
        self.logger.info("TimeSyncServer: Server has been closed.")
        TimeSyncServer._instance = None  # Reset the singleton instance
