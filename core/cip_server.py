import socket
import threading
from datetime import datetime
import statistics
class CIPServer:
    def __init__(self, logger, consumer_config):
        self.logger = logger
        self.consumer_config = consumer_config
        self.tcp_socket = None
        self.udp_socket = None
        self.running = False
        self.last_sequence_numbers = {}
        self.flight_times = []  # Store recent flight times for standard deviation calculation
        self.max_flight_time_samples = 500  # Maximum number of flight times to store
        
    def start(self):
        """Start the TCP and UDP server."""
        self.running = True
        self.logger("CIP Server is starting...", level="INFO")
        threading.Thread(target=self.handle_tcp_connection, daemon=True).start()
        threading.Thread(target=self.handle_udp_io, daemon=True).start()

    def stop(self):
        """Stop the TCP and UDP server."""
        self.running = False
        if self.tcp_socket:
            self.tcp_socket.close()
            self.tcp_socket = None
        if self.udp_socket:
            self.udp_socket.close()
            self.udp_socket = None
        self.logger("CIP Server stopped.", level="INFO")

    def handle_tcp_connection(self):
        """Handle the TCP CIP Connection Management server."""
        conn = None
        try:
            self.logger("Initializing TCP socket...", level="INFO")
            self.tcp_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.tcp_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            self.tcp_socket.bind(("", self.consumer_config.get("tcp_port", 1502)))
            self.tcp_socket.listen(1)
            self.tcp_socket.settimeout(0.5)
            self.logger(f"TCP server is listening on port {self.consumer_config.get('tcp_port', 1502)}.", level="INFO")

            while self.running:
                try:
                    conn, addr = self.tcp_socket.accept()
                    conn.settimeout(0.5)
                    self.logger(f"TCP connection established with {addr}.", level="INFO")

                    while self.running:
                        try:
                            data = conn.recv(1024).decode()
                            if data == "DISCONNECT":
                                self.logger(f"Client {addr} requested disconnect.", level="INFO")
                                break
                            elif data:
                                self.logger(f"Received keepalive from client: {data}", level="INFO")
                        except socket.timeout:
                            continue
                except socket.timeout:
                    continue
                except OSError as e:
                    self.logger(f"TCP connection error: {e}", level="ERROR")
                    break
        finally:
            if conn:
                conn.close()
            if self.tcp_socket:
                self.tcp_socket.close()
                self.logger("TCP server shut down.", level="INFO")

    def _check_flight_time_outlier(self, flight_time_ms):
        """Check if a flight time is an outlier based on mean and standard deviation."""
        # Add the new flight time to the list
        self.flight_times.append(flight_time_ms)
        
        # Trim the list to the maximum number of samples
        if len(self.flight_times) > self.max_flight_time_samples:
            self.flight_times.pop(0)

        # Calculate mean and standard deviation
        if len(self.flight_times) >= 2:  # Require at least two samples to calculate std deviation
            mean_flight_time = statistics.mean(self.flight_times)
            stdev_flight_time = statistics.stdev(self.flight_times)

            # Consider the current flight time an outlier if it exceeds mean + 2 * stdev
            threshold = mean_flight_time + 2 * stdev_flight_time
            is_outlier = flight_time_ms > threshold
            return is_outlier, mean_flight_time, stdev_flight_time
        else:
            # Not enough data to calculate outliers yet
            return False, None, None

    def handle_udp_io(self):
        """Handle the UDP I/O server."""
        try:
            self.logger("Initializing UDP socket...", level="INFO")
            self.udp_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            self.udp_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            self.udp_socket.bind(("", self.consumer_config.get("udp_port", 2222)))
            self.udp_socket.settimeout(0.5)
            self.logger(f"UDP server is listening on port {self.consumer_config.get('udp_port', 2222)}.", level="INFO")

            while self.running:
                try:
                    data, addr = self.udp_socket.recvfrom(self.consumer_config.get("buffer_size", 2048))
                    #print(f"[DEBUG] Packet received from {addr}: {data.decode()}")
                    
                    # Capture receive timestamp
                    rcvd_timestamp = datetime.now()
                    tag, received_seq_num, sent_timestamp = data.decode().split(',')
                    received_seq_num = int(received_seq_num)
                    
                    # Convert sent timestamp to a datetime object
                    packet_timestamp = datetime.strptime(sent_timestamp, "%Y-%m-%d %H:%M:%S.%f")

                    # Calculate flight time in milliseconds
                    flight_time_ms = (rcvd_timestamp - packet_timestamp).total_seconds() * 1000

                    # Check if flight time is an outlier
                    is_outlier, mean, stdev = self._check_flight_time_outlier(flight_time_ms)

                    # Track last sequence number per client tag
                    last_seq_num = self.last_sequence_numbers.get(tag, 0)
                    if received_seq_num != last_seq_num + 1 and last_seq_num != 0:
                        missed_count = received_seq_num - last_seq_num - 1
                        #print(f"[DEBUG] Missed {missed_count} packets from tag '{tag}' between {last_seq_num} and {received_seq_num}")
                        missed_log = (
                            f"Missed MISSEDCNT={missed_count} packet(s)  TAG='{tag}' SRC_IP_PORT={addr} MISS_TIMESTAMP={packet_timestamp} "
                            f"between sequence LSEQNO={last_seq_num} and RSEQNO={received_seq_num} "
                        )
                        self.logger(missed_log, level="ERROR")
                    # Log the received packet, marking outliers if necessary
                    received_log = (
                        f"Received SEQNO={received_seq_num} TAG='{tag}' SRC_IP_PORT={addr} "
                        f"SENT_TIMESTAMP={sent_timestamp} RCVD_TIMESTAMP={rcvd_timestamp.strftime('%Y-%m-%d %H:%M:%S.%f')} "
                        f"FLIGHT_TIME={flight_time_ms:.3f} ms"
                    )

                    if is_outlier:
                        received_log += f" [OUTLIER - Mean: {mean:.3f} ms, StDev: {stdev:.3f} ms]"
                        # Pass the `outlier_tag` for color coding if configured in GUI/CLI logger
                        self.logger(received_log, level="ERROR")
                    else:
                        self.logger(received_log, level="INFO")

                    # Update last sequence number for this client tag
                    self.last_sequence_numbers[tag] = received_seq_num

                except socket.timeout:
                    continue
                except OSError as e:
                    self.logger(f"UDP server error: {e}", level="ERROR")
                    break
        finally:
            if self.udp_socket:
                self.udp_socket.close()
                self.logger("UDP server shut down.", level="INFO")