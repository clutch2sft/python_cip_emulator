import time
import grpc
import threading
from datetime import datetime
from core.time.driftcorrection import DriftCorrectorBorg
from core.time.latencysmoother import LatencySmoother
from core.time.server import timesync_pb2
from core.time.server import timesync_pb2_grpc

class TimeSyncClient:
    def __init__(self, server_ip, logger_app, server_port=5555, network_latency_ns=30000000, txrate=0.1, debug=False, filter_factor=2):
        self.server_address = f"{server_ip}:{server_port}"
        self.logger_app = logger_app
        self.network_latency_ns = network_latency_ns
        self.txrate = txrate
        self.debug = debug
        self.server_reachable = False
        self.running = False
        self.stability_detected = False
        self.idle_mode = False  # New: Tracks if the client is in idle mode
        self.condition = threading.Condition()
        self.class_name = self.__class__.__name__

        # Initialize gRPC channel and stubs
        self.channel = grpc.insecure_channel(self.server_address)
        self.stub = timesync_pb2_grpc.TimeSyncServiceStub(self.channel)

        # Initialize drift corrector and latency smoother
        self.drift_corrector = DriftCorrectorBorg(network_latency_ns=network_latency_ns, logger_app=self.logger_app)
        self.latency_smoother = LatencySmoother(max_samples=100, filter_factor=filter_factor, logger_app=self.logger_app)

    def check_health(self):
        try:
            response = self.stub.CheckHealth(timesync_pb2.HealthCheckRequest(), timeout=5.0)
            self.server_reachable = response.healthy
            self.logger_app.info(
                f"{self.class_name}: Health check successful. "
                f"Server Uptime: {response.uptime_seconds}s, "
                f"Requests Handled: {response.request_count}, "
                f"Average Response Time: {response.avg_response_time_ms:.2f}ms"
            )
            return response.healthy
        except grpc.RpcError as e:
            self.logger_app.error(f"{self.class_name}: Health check failed with error: {e}")
            self.server_reachable = False
            return False

    def start(self):
        """Start the time synchronization process."""
        if self.check_health():
            self.running = True
            self.idle_mode = False  # Reset idle mode
            self.thread = threading.Thread(target=self._run, daemon=True)
            self.thread.start()
        else:
            self.logger_app.error(f"{self.class_name}: Could not start due to unreachable server.")
        return self.server_reachable

    def stop(self):
        """Stop the time synchronization process."""
        self.logger_app.info(f"{self.class_name} stop(): Stop called.")
        self.running = False
        if self.thread is not None and self.thread.is_alive():
            self.thread.join(timeout=5)
        self.logger_app.info(f"{self.class_name} stop(): Thread joined if it was running.")

    def _run(self):
        retries = 0
        max_retries = 3
        unstable_count = 0
        stable_count = 0
        stable_threshold = 30
        initial_txrate = self.txrate
        adaptive_lower_txrate_factor = 5
        lower_txrate = initial_txrate * adaptive_lower_txrate_factor

        self.logger_app.info(f"{self.class_name}: Time Sync Client started")

        while self.running:
            try:
                # Stop syncing if idle mode is activated
                if self.idle_mode:
                    self.logger_app.info(f"{self.class_name}: Entering idle mode after stability.")
                    self.close()  # Close gRPC connection
                    break

                # Request timestamp from the server
                client_request_time_ns = time.time_ns()
                response = self.stub.RequestTimestamp(timesync_pb2.TimeRequest(), timeout=5.0)
                server_timestamp_ns = response.timestamp_ns
                client_receive_time_ns = time.time_ns()

                # Process response, calculate round-trip time, and adjust server timestamp
                round_trip_time_ns = client_receive_time_ns - client_request_time_ns
                self.latency_smoother.add_rtt_sample(round_trip_time_ns)
                smoothed_latency_ns, half_smoothed_latency = self.latency_smoother.get_smoothed_latency()

                if smoothed_latency_ns > 0:
                    adjusted_server_timestamp_ns = server_timestamp_ns + half_smoothed_latency
                    discrepancy_ns = client_receive_time_ns - adjusted_server_timestamp_ns
                    client_ahead = discrepancy_ns > 0
                    self.drift_corrector.add_discrepancy(abs(discrepancy_ns), client_ahead)

                    relative_deviation = abs(round_trip_time_ns - (half_smoothed_latency * 2)) / (half_smoothed_latency * 2)
                    relative_deviation_threshold = 0.5

                if relative_deviation > relative_deviation_threshold:
                    unstable_count += 1
                    stable_count = 0
                    if unstable_count >= stable_threshold:
                        self.txrate = initial_txrate
                        self.stability_detected = False
                        self.logger_app.warning(
                            f"{self.class_name}: Instability detected, txrate adjusted to {initial_txrate}, "
                            f"RTT: {round_trip_time_ns / 1_000_000} ms"
                        )
                else:
                    unstable_count = 0  # Reset unstable count when stable
                    stable_count += 1
                    if not self.stability_detected and stable_count >= stable_threshold:
                        self.txrate = max(lower_txrate, smoothed_latency_ns / 1_000_000 * 0.005)
                        self.stability_detected = True
                        self.logger_app.info(f"{self.class_name}: Stability detected, txrate adjusted to {self.txrate}")

                        # Transition to idle mode
                        self.idle_mode = True

                retries = 0  # Reset retries on success
                time.sleep(self.txrate)

            except grpc.RpcError as e:
                retries += 1
                self.logger_app.error(f"{self.class_name}: gRPC error occurred: {e}")
                if retries >= max_retries:
                    self.logger_app.error(f"{self.class_name}: Maximum retries reached. Stopping client.")
                    self.stop()
                    break

    def is_stable(self):
        return self.stability_detected

    def get_corrected_time(self):
        corrected_drift_ns = self.drift_corrector.calculate_mean_drift()
        client_time_ns = time.time_ns()
        corrected_client_time_ns = client_time_ns + corrected_drift_ns
        return corrected_client_time_ns

    def close(self):
        """Close the gRPC channel."""
        if self.channel:
            self.channel.close()
            self.logger_app.info(f"{self.class_name}: gRPC channel closed.")
