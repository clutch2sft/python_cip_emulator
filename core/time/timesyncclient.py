import zmq
import time
from datetime import datetime
import threading
from core.time.driftcorrection import DriftCorrectorBorg
from core.time.latencysmoother import LatencySmoother
from zmq import EVENT_CONNECTED, EVENT_DISCONNECTED, EVENT_CLOSED
from textwrap import dedent

class TimeSyncClient:
    def __init__(self, server_ip, logger_app, server_port=5555, network_latency_ns=30000000, txrate=0.1, debug=False, filter_factor=2):
        self.server_ip = server_ip
        self.server_port = server_port
        self.logger_app = logger_app
        self.network_latency_ns = network_latency_ns
        self.txrate = txrate
        self.debug = debug
        self.context = zmq.Context()
        self.socket = self.context.socket(zmq.REQ)
        self.socket.setsockopt(zmq.LINGER, 0)  # Set linger to 0 to avoid blocking on close
        self.socket.connect(f"tcp://{self.server_ip}:{self.server_port}")
        # Start monitoring connection status
        self.setup_socket_monitor()
        self.running = False
        self.stability_detected = False  # Stability flag
        self.server_reachable = False
        self.thread = None  # Initialize thread attribute to None
        self.class_name = self.__class__.__name__
        # Initialize drift corrector and latency smoother
        self.drift_corrector = DriftCorrectorBorg(network_latency_ns=network_latency_ns, logger_app=self.logger_app)
        self.latency_smoother = LatencySmoother(max_samples=100, filter_factor=filter_factor, logger_app=self.logger_app)
        self.initialized = True

    def setup_socket_monitor(self):
        monitor_socket = self.socket.get_monitor_socket()
        
        def monitor_events():
            while True:
                try:
                    # Receive the multipart message from the monitor socket
                    event_parts = monitor_socket.recv_multipart(zmq.NOBLOCK)
                    
                    # Parse the event ID
                    event_id = int.from_bytes(event_parts[0], byteorder='little')
                    
                    if event_id == EVENT_CONNECTED:
                        self.logger_app.info("{self.class_name}: Successfully connected to server.")
                        break
                    elif event_id in [EVENT_DISCONNECTED, EVENT_CLOSED]:
                        self.logger_app.error("{self.class_name}: Server unreachable or connection failed.")
                        break
                except zmq.Again:
                    continue
                except IndexError:
                    self.logger_app.error("{self.class_name}: Malformed event message.")
                    break

        threading.Thread(target=monitor_events, daemon=True).start()

    def test_connection(self):
        try:
            # Setup monitor to detect unreachable server
            self.setup_socket_monitor()

            # Send a test request
            self.socket.send(b"REQUEST_TIMESTAMP", zmq.NOBLOCK)
            
            # Wait for a response with poll, and handle unreachable status via monitor
            if self.socket.poll(5000, zmq.POLLIN):  # Wait up to 5 seconds
                _ = self.socket.recv(zmq.NOBLOCK)
                return True
            else:
                self.logger_app.error(f"{self.class_name}: Server is reachable, but not responding within timeout.")
        except zmq.error.ZMQError as e:
            self.logger_app.error(f"{self.class_name}: Connection test failed with error: {e}")
        return False


    def test_connection(self):
        try:
            # Attempt to send "REQUEST_TIMESTAMP" to test server connectivity
            self.socket.send(b"REQUEST_TIMESTAMP", zmq.NOBLOCK)
            
            # Poll with a timeout to wait for the server's response
            if self.socket.poll(5000, zmq.POLLIN):  # Wait for up to 5 seconds
                _ = self.socket.recv(zmq.NOBLOCK)  # Receive and discard response to maintain REQ/REP state
                return True
            else:
                self.logger_app.error("TimeSyncClient: Server reachable, but no response within timeout.")
        except zmq.error.ZMQError as e:
            self.logger_app.error(f"{self.class_name}: Connection test failed with error: {e}")
        return False

    def start(self):
        if self.test_connection():
            self.server_reachable = True
            self.running = True
            self.thread = threading.Thread(target=self._run)
            self.thread.start()
        else:
            self.logger_app.error("{self.class_name}: Could not start due to unreachable server.")
        return self.server_reachable
        
    def stop(self):
        self.logger_app.info(f"{self.class_name} stop(): Stop Called.")
        self.running = False
        if self.thread is not None and self.thread.is_alive():
            self.thread.join(timeout=5)
        self.logger_app.info(f"{self.class_name} stop(): Thread joined (if it was running).")


    def is_stable(self):
        """Return whether stability has been detected."""
        if self.stability_detected:
            self.logger_app.info(f"{self.class_name}: is_stable: Returning Stability is detected by time sync client.")
        else:
            if self.debug:
                self.logger_app.info(f"{self.class_name}: NOT STABLE Returning same.")
        return self.stability_detected

    def format_timestamp_ns(self, timestamp_ns):
        # Convert the nanosecond timestamp to seconds
        timestamp_sec = timestamp_ns / 1_000_000_000
        # Create a datetime object from the seconds timestamp
        dt = datetime.fromtimestamp(timestamp_sec)
        # Format the datetime object into a readable string
        formatted_time = dt.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]  # Trim to milliseconds
        return formatted_time

    def _run(self):
        unstable_count = 0  # Count of consecutive unstable samples
        stable_count = 0    # Count of consecutive stable samples
        stable_threshold = 3  # Number of stable samples needed to confirm stability
        initial_txrate = self.txrate  # Use initial txrate for quick sampling
        adaptive_lower_txrate_factor = 5  # Factor to apply to initial txrate for lower rate

        # Dynamically calculated lower txrate after initial stabilization
        lower_txrate = initial_txrate * adaptive_lower_txrate_factor

        self.logger_app.info(f"{self.class_name}: Time Sync Client started")

        while self.running:
            try:
                # Send the timestamp request
                #client_request_time_ns = time.perf_counter_ns()
                client_request_time_ns = time.time_ns()
                self.socket.send(b"REQUEST_TIMESTAMP")

                # Wait for a response with a timeout (e.g., 5 seconds)
                if self.socket.poll(5000, zmq.POLLIN):  # Wait for up to 5 seconds for a reply
                    server_timestamp_ns = self.socket.recv_pyobj()  # This is now non-blocking due to poll
                    #client_receive_time_ns = time.perf_counter_ns()
                    client_receive_time_ns = time.time_ns()

                    # Convert nanoseconds to a datetime object
                    client_request_time_s = client_request_time_ns / 1_000_000_000  # Convert to seconds
                    client_request_datetime = datetime.fromtimestamp(client_request_time_s)

                    # Format the datetime with nanoseconds
                    formatted_time = client_request_datetime.strftime('%Y-%m-%d %H:%M:%S') + f".{client_request_time_ns % 1_000_000_000:09d}"

                    # Process the response as before
                    # Calculate round-trip time and add it to the latency smoother
                    round_trip_time_ns = client_receive_time_ns - client_request_time_ns
                    self.latency_smoother.add_rtt_sample(round_trip_time_ns)

                    # Use the smoothed latency (one-way) and adjust server timestamp
                    smoothed_latency_ns, half_smoothed_latency = self.latency_smoother.get_smoothed_latency()

                    if smoothed_latency_ns > 0:  # Only proceed if latency is non-zero
                        adjusted_server_timestamp_ns = server_timestamp_ns + half_smoothed_latency

                        # Calculate and correct timing discrepancy
                        discrepancy_ns = client_receive_time_ns - adjusted_server_timestamp_ns
                        client_ahead = discrepancy_ns > 0
                        self.drift_corrector.add_discrepancy(abs(discrepancy_ns), client_ahead)

                        # Calculate relative deviation to determine latency stability
                        relative_deviation_threshold = 0.5  # Example threshold: 30%
                        relative_deviation = abs(round_trip_time_ns - (half_smoothed_latency * 2)) / (half_smoothed_latency * 2)

                        if relative_deviation > relative_deviation_threshold:
                            # Detected instability (e.g., roaming event or latency spike)
                            unstable_count += 1
                            self.logger_app.info(f"{self.class_name}: Unstable entry found Stable Count:{stable_count} Unstable Count: {unstable_count}")
                            stable_count -= stable_count  # Reset stable count on instability
                            if unstable_count >= stable_threshold:
                                stable_count = 0
                                self.txrate = initial_txrate  # Temporarily increase txrate for frequent sampling
                                self.stability_detected = False  # Reset stability flag due to detected instability
                                if unstable_count == stable_threshold + 1:
                                    self.logger_app.warning(
                                        f"{formatted_time} {self.class_name}: Instability detected, txrate adjusted to {initial_txrate} "
                                        f"Unstable count:{unstable_count} Relative Deviation: {relative_deviation} RTT: {round_trip_time_ns / 1_000_000}"
                                    )
                                if self.debug:
                                    self.logger_app.info(f"{self.class_name}: Relative deviation is {relative_deviation}")
                                    self.logger_app.info(
                                        f"Client Request Time:{self.format_timestamp_ns(client_request_time_ns)} "
                                        f"Client Receive Time:{self.format_timestamp_ns(client_receive_time_ns)} "
                                        f"Server Time:{self.format_timestamp_ns(server_timestamp_ns)} "
                                        f"Smoothed Latency/Half Smoothed Latency/RTT: {smoothed_latency_ns / 1_000_000}ms/{half_smoothed_latency / 1_000_000}ms/{round_trip_time_ns / 1_000_000}ms "
                                        f"Adjusted Server Time:{self.format_timestamp_ns(adjusted_server_timestamp_ns)} "
                                        f"Relative Deviation:{relative_deviation} "
                                        f"Deviation Threshold:{relative_deviation_threshold} "
                                    )
                        else:
                            # Detected stability
                            if not self.stability_detected:  # Only adjust if stability wasn't previously detected
                                stable_count += 1
                                self.logger_app.info(f"{self.class_name}: Stability detected, stable count {stable_count} {formatted_time}")
                                if stable_count >= stable_threshold:
                                    # Once stable, set txrate based on smoothed latency and initial_txrate
                                    lower_txrate = max(initial_txrate * adaptive_lower_txrate_factor, 
                                                    smoothed_latency_ns / 1_000_000 * 0.005)  # Example dynamic adjustment
                                    self.txrate = lower_txrate  # Lower txrate for stable conditions
                                    self.stability_detected = True  # Set stability flag after reaching stable conditions
                                    self.logger_app.info(f"{self.class_name}: Stability detected, txrate adjusted to {lower_txrate} {formatted_time}")
                            else:
                                stable_count += 1  # Increment stable count but avoid redundant adjustments

                    else:
                        self.logger_app.warning(f"{self.class_name}: Smoothed latency is zero; skipping stability calculations.")

                else:
                    # If no response is received, log a warning and check `self.running`
                    self.logger_app.warning(f"{self.class_name}: No response from server, retrying...")

            except zmq.error.ZMQError as e:
                self.logger_app.error(f"{self.class_name}: ZeroMQ error occurred: {e}")
                break

            # Wait based on current txrate, checking `self.running`
            time.sleep(self.txrate)
            
        self.logger_app.info(f"{self.class_name}: Time sync client stopped.")



    def get_corrected_time(self):
        corrected_drift_ns = self.drift_corrector.calculate_mean_drift()
        client_time_ns = time.time_ns()
        corrected_client_time_ns = client_time_ns + corrected_drift_ns
        return corrected_client_time_ns

    def close(self):
        self.socket.close()
        self.context.term()
