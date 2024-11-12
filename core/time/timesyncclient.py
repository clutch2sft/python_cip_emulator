import zmq, time, threading
from collections import deque
from driftcorrection import DriftCorrector

class TimeSyncClient:
    def __init__(self, server_ip, server_port=5555, network_latency_ns=30000000):
        self.server_ip = server_ip
        self.server_port = server_port
        self.network_latency_ns = network_latency_ns
        self.context = zmq.Context()
        self.socket = self.context.socket(zmq.PAIR)
        self.socket.connect(f"tcp://{self.server_ip}:{self.server_port}")
        self.running = False

        # Initialize the drift corrector with a buffer size of 250 samples
        self.drift_corrector = DriftCorrector(network_latency_ns=network_latency_ns)

    def start(self):
        """Start the client and begin receiving timestamps."""
        self.running = True
        threading.Thread(target=self._run, daemon=True).start()

    def stop(self):
        """Stop the client."""
        self.running = False

    def _run(self):
        while self.running:
            # Receive the server's timestamp
            server_timestamp_ns = self.socket.recv_pyobj()
            client_timestamp_ns = time.time_ns()
            discrepancy_ns = client_timestamp_ns - server_timestamp_ns
            client_ahead = discrepancy_ns > 0

            # Add the discrepancy to the drift corrector
            self.drift_corrector.add_discrepancy(abs(discrepancy_ns), client_ahead)
            print(f"Client received server timestamp: {server_timestamp_ns}")
            print(f"Timing Discrepancy: {discrepancy_ns} ns, Client ahead: {client_ahead}")

            time.sleep(0.1)  # Check every 100 ms

    def get_corrected_time(self):
        """Calculate and return the corrected client time."""
        corrected_drift_ns = self.drift_corrector.calculate_mean_drift()
        client_time_ns = time.time_ns()
        corrected_client_time_ns = client_time_ns + corrected_drift_ns
        return corrected_client_time_ns

    def close(self):
        """Close the ZeroMQ socket and context."""
        self.socket.close()
        self.context.term()
