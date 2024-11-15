# rtt_client.py
import socket
import time
import threading
from core.time.scratch.net_latency import NetLatency

class RTTClient:
    _instance = None
    _lock = threading.Lock()  # Thread-safe singleton lock

    def __new__(cls, *args, **kwargs):
        if not cls._instance:
            with cls._lock:
                if not cls._instance:
                    cls._instance = super(RTTClient, cls).__new__(cls)
        return cls._instance

    def __init__(self, host='127.0.0.1', port=65432, protocol='udp', num_pings=250):
        if not hasattr(self, 'initialized'):  # Ensure `__init__` runs only once
            self.host = host
            self.port = port
            self.protocol = protocol.lower()
            self.num_pings = num_pings
            self.latency_tracker = NetLatency()
            self.running = False  # Track whether the client is currently running
            self.initialized = True
            self._thread = None  # Thread for running start() non-blocking

    def start(self):
        """Start the RTT measurement process in a non-blocking way."""
        with RTTClient._lock:  # Thread-safe check and set for `running`
            if self.running:
                print("RTTClient is already running.")
                return

            self.running = True
            # Start a separate thread for the RTT measurement process
            self._thread = threading.Thread(target=self._run_rtt_measurement)
            self._thread.start()

    def _run_rtt_measurement(self):
        """Internal method to perform RTT measurements."""
        sock_type = socket.SOCK_STREAM if self.protocol == 'tcp' else socket.SOCK_DGRAM

        try:
            with socket.socket(socket.AF_INET, sock_type) as client_socket:
                client_socket.settimeout(1)  # Set a 1-second timeout

                # Establish connection for TCP
                if self.protocol == 'tcp':
                    client_socket.connect((self.host, self.port))

                for i in range(self.num_pings):
                    if not self.running:  # Check if stop() was called
                        break

                    try:
                        send_time = time.time()

                        if self.protocol == 'udp':
                            client_socket.sendto(b"ping", (self.host, self.port))
                            data, _ = client_socket.recvfrom(1024)
                        elif self.protocol == 'tcp':
                            client_socket.sendall(b"ping")
                            data = client_socket.recv(1024)

                        receive_time = time.time()
                        rtt = (receive_time - send_time) * 1000  # RTT in milliseconds
                        print(f"Ping {i+1}: RTT = {rtt:.2f} ms")
                        self.latency_tracker.add_rtt(rtt)

                    except socket.timeout:
                        print(f"Ping {i+1}: Request timed out")
                        self.latency_tracker.add_rtt(None)  # Track timeout as None

        finally:
            # Ensure that the client can be started again after finishing
            self.running = False

    def stop(self):
        """Stop the RTT measurement process if it is running."""
        with RTTClient._lock:
            if self.running:
                print("Stopping RTTClient...")
                self.running = False  # This will stop the measurement loop
                if self._thread:
                    self._thread.join()  # Wait for the thread to finish
                print("RTTClient stopped.")

    def get_latency_statistics(self):
        """Retrieve RTT statistics from NetLatency."""
        return self.latency_tracker.get_statistics()

    def clear_latency_data(self):
        """Clear all RTT data in NetLatency."""
        self.latency_tracker.clear()
