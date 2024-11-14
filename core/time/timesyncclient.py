import time
import grpc
import threading
from datetime import datetime
from core.time.driftcorrection import DriftCorrectorBorg
from core.time.latencysmoother import LatencySmoother
import timesync_pb2
import timesync_pb2_grpc


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
        self.thread = None
        self.class_name = self.__class__.__name__
        
        # Initialize gRPC channel and stubs
        self.channel = grpc.insecure_channel(self.server_address)
        self.stub = timesync_pb2_grpc.TimeSyncServiceStub(self.channel)
        
        # Initialize drift corrector and latency smoother
        self.drift_corrector = DriftCorrectorBorg(network_latency_ns=network_latency_ns, logger_app=self.logger_app)
        self.latency_smoother = LatencySmoother(max_samples=100, filter_factor=filter_factor, logger_app=self.logger_app)
        
    def check_health(self):
        """Check server connectivity via a health check RPC."""
        try:
            response = self.stub.CheckHealth(timesync_pb2.HealthCheckRequest(), timeout=5.0)
            self.server_reachable = response.healthy
            return response.healthy
        except grpc.RpcError as e:
            self.logger_app.error(f"{self.class_name}: Health check failed with error: {e}")
            self.server_reachable = False
            return False

    def start(self):
        """Start the time synchronization process in a background thread."""
        if self.check_health():
            self.running = True
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
        unstable_count = 0
        stable_count = 0
        stable_threshold = 3
        initial_txrate = self.txrate
        adaptive_lower_txrate_factor = 5
        lower_txrate = initial_txrate * adaptive_lower_txrate_factor

        self.logger_app.info(f"{self.class_name}: Time Sync Client started")

        while self.running:
            try:
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
                            if self.debug:
                                self.logger_app.warning(
                                    f"{self.class_name}: Instability detected, txrate adjusted to {initial_txrate}, "
                                    f"RTT: {round_trip_time_ns / 1_000_000} ms"
                                )
                    else:
                        if not self.stability_detected:
                            stable_count += 1
                            if stable_count >= stable_threshold:
                                self.txrate = max(lower_txrate, smoothed_latency_ns / 1_000_000 * 0.005)
                                self.stability_detected = True
                                self.logger_app.info(f"{self.class_name}: Stability detected, txrate adjusted to {self.txrate}")

                time.sleep(self.txrate)

            except grpc.RpcError as e:
                self.logger_app.error(f"{self.class_name}: gRPC error occurred: {e}")
                self.stop()  # Stop on critical error
                break

    def get_corrected_time(self):
        corrected_drift_ns = self.drift_corrector.calculate_mean_drift()
        client_time_ns = time.time_ns()
        corrected_client_time_ns = client_time_ns + corrected_drift_ns
        return corrected_client_time_ns

    def close(self):
        self.channel.close()
