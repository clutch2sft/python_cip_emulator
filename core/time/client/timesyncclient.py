import time
import grpc
import threading
from datetime import datetime
from core.time.driftcorrection import DriftCorrectorBorg
from core.time.latencysmoother import LatencySmoother
from core.time.server import timesync_pb2
from  core.time.server import timesync_pb2_grpc
from concurrent.futures import ThreadPoolExecutor

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
        #could not get stability so trying without thread pool
        #self.thread_pool = ThreadPoolExecutor(max_workers=20)  # Shared thread pool
        self.min_samples_required = 50
        self.warm_up_samples = 60
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

    # Pooled start method:
    # def start(self):
    #     if self.check_health():
    #         self.running = True
    #         self.thread_pool.submit(self._run)
    #     else:
    #         self.logger_app.error(f"{self.class_name}: Could not start due to unreachable server.")
    #     return self.server_reachable

    def start(self):
        """Start the time synchronization process in a dedicated thread."""
        if self.check_health():
            self.running = True
            self.thread = threading.Thread(target=self._run, daemon=True)
            self.thread.start()
        else:
            self.logger_app.error(f"{self.class_name}: Could not start due to unreachable server.")
        return self.server_reachable

    # Pooled stop method:
    # def stop(self):
    #     self.logger_app.info(f"{self.class_name}: Drift sample count: {self.drift_corrector.get_sample_count()}")
    #     self.logger_app.info(f"{self.class_name}: Latency sample count: {self.latency_smoother.get_sample_count()}")
    #     self.logger_app.info(f"{self.class_name} stop(): Stop called.")
    #     #self.logger_app.info(f"{self.class_name}: Drift sample count: {self.get_sample_count()}")
    #     self.running = False
    #     self.thread_pool.shutdown(wait=True)
    #     self.logger_app.info(f"{self.class_name} stop(): All threads joined.")

    def stop(self):
        """Stop the time synchronization process."""
        self.logger_app.info(f"{self.class_name} stop(): Stop called.")
        self.running = False
        if self.thread is not None and self.thread.is_alive():
            self.thread.join(timeout=5)
        self.logger_app.info(f"{self.class_name} stop(): Thread joined if it was running.")

    # Pooled _run method:
    # def _run(self):
    #     stable_count = 0
    #     unstable_count = 0
    #     stable_threshold = 3

    #     self.logger_app.info(f"{self.class_name}: Time Sync Client started")

    #     while self.running:
    #         with self.condition:
    #             self.condition.wait(timeout=self.txrate)
    #             if not self.running:
    #                 break
    #         try:
    #             # Request timestamp from the server
    #             client_request_time_ns = time.time_ns()
    #             response = self.stub.RequestTimestamp(timesync_pb2.TimeRequest(), timeout=5.0)
    #             server_timestamp_ns = response.timestamp_ns
    #             client_receive_time_ns = time.time_ns()

    #             # Process response, calculate round-trip time, and adjust server timestamp
    #             round_trip_time_ns = client_receive_time_ns - client_request_time_ns

    #             if self.latency_smoother.get_sample_count() < self.warm_up_samples:
    #                 self.latency_smoother.add_rtt_sample(round_trip_time_ns)
    #                 self.logger_app.info(
    #                     f"{self.class_name}: Collecting warm-up samples ({self.latency_smoother.get_sample_count()}/{self.warm_up_samples})"
    #                 )
    #                 continue

    #             smoothed_latency_ns, half_smoothed_latency = self.latency_smoother.get_smoothed_latency()
                
    #             if smoothed_latency_ns > 0:
    #                 adjusted_server_timestamp_ns = server_timestamp_ns + half_smoothed_latency
    #                 discrepancy_ns = client_receive_time_ns - adjusted_server_timestamp_ns
    #                 client_ahead = discrepancy_ns > 0

    #                 # Perform drift correction
    #                 self.drift_corrector.add_discrepancy(abs(discrepancy_ns), client_ahead)


    #                 # Stability analysis
    #                 relative_deviation = abs(round_trip_time_ns - (half_smoothed_latency * 2)) / (half_smoothed_latency * 2)
    #                 relative_deviation_threshold = 0.5

    #                 if relative_deviation > relative_deviation_threshold:
    #                     unstable_count += 1
    #                     stable_count = 0
    #                     if unstable_count >= stable_threshold:
    #                         self.txrate = 0.1  # Reset to initial txrate
    #                         self.stability_detected = False
    #                         self.logger_app.warning(
    #                             f"{self.class_name}: Instability detected. "
    #                             f"RTT: {round_trip_time_ns / 1_000_000} ms, txrate reset to {self.txrate}"
    #                         )
    #                 else:
    #                     stable_count += 1
    #                     if stable_count >= stable_threshold and not self.stability_detected:
    #                         self.txrate = max(0.005, smoothed_latency_ns / 1_000_000 * 0.005)
    #                         self.stability_detected = True
    #                         self.logger_app.info(
    #                             f"{self.class_name}: Stability detected. RTT: {round_trip_time_ns / 1_000_000} ms, txrate adjusted to {self.txrate}"
    #                         )

    #             time.sleep(self.txrate)

    #         except grpc.RpcError as e:
    #             self.logger_app.error(f"{self.class_name}: gRPC error occurred: {e}")
    #             self.running = False
    def _run(self):
        retries = 0
        max_retries = 3
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

                # Decay Factor for Stability:

                # When instability occurs, reduce stable_count by a percentage (e.g., 50%), instead of resetting to zero.
                # For very stable conditions (stable_count > stable_threshold), this allows the system to recover faster after transient instabilities.
                # Hard Reset for Low Stability:

                # If stable_count is already below stable_threshold, reset to zero to quickly handle persistent instabilities.
                # Reset unstable_count on Stability:

                # Reset unstable_count when relative deviation is below the threshold to indicate regained stability.

                # The decay_factor (e.g., 0.5) can be adjusted based on your system's sensitivity to instability:

                # Higher decay factor (e.g., 0.7): More aggressive stability decay during instability.
                # Lower decay factor (e.g., 0.3): More forgiving to minor deviations, retaining higher stability counts.

                if relative_deviation > relative_deviation_threshold:
                    unstable_count += 1

                    # Gradual reduction of stable count instead of direct reset to zero
                    if stable_count > stable_threshold:
                        decay_factor = 0.5  # Decay 50% of the current stable count
                        stable_count = int(stable_count * (1 - decay_factor))
                    else:
                        stable_count = 0  # Hard reset if stability is low

                    # Handle instability threshold
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

                    # Trigger stability logic only if not already stable
                    if not self.stability_detected and stable_count >= stable_threshold:
                        self.txrate = max(lower_txrate, smoothed_latency_ns / 1_000_000 * 0.005)
                        self.stability_detected = True
                        self.logger_app.info(f"{self.class_name}: Stability detected, txrate adjusted to {self.txrate}")


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
        if self.stability_detected:
            self.logger_app.info(f"{self.class_name}: Stability is detected.")
        elif self.debug:
            self.logger_app.info(f"{self.class_name}: Stability not detected.")
        return self.stability_detected

    def get_corrected_time(self):
        corrected_drift_ns = self.drift_corrector.calculate_mean_drift()
        client_time_ns = time.time_ns()
        corrected_client_time_ns = client_time_ns + corrected_drift_ns
        return corrected_client_time_ns

    def close(self):
        if self.channel:
            self.channel.close()
            self.logger_app.info(f"{self.class_name}: gRPC channel closed.")
