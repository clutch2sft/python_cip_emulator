import socket
import threading
from datetime import datetime

class CIPServer:
    def __init__(self, logger, consumer_config):
        self.logger = logger
        self.consumer_config = consumer_config
        self.tcp_socket = None
        self.udp_socket = None
        self.running = False
        self.last_sequence_numbers = {}

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

    def handle_udp_io(self):
        """Handle the UDP I/O server."""
        try:
            #print("[DEBUG] Starting UDP socket initialization...")
            self.logger("Initializing UDP socket...", level="INFO")
            self.udp_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            self.udp_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            self.udp_socket.bind(("", self.consumer_config.get("udp_port", 2222)))
            self.udp_socket.settimeout(0.5)
            
            udp_port = self.consumer_config.get("udp_port", 2222)
            #print(f"[DEBUG] UDP server listening on port {udp_port}")
            self.logger(f"UDP server is listening on port {udp_port}.", level="INFO")

            while self.running:
                try:
                    #print("[DEBUG] Waiting for UDP packet...")
                    data, addr = self.udp_socket.recvfrom(self.consumer_config.get("buffer_size", 2048))
                    #print(f"[DEBUG] Packet received from {addr}: {data.decode()}")

                    tag, received_seq_num, timestamp = data.decode().split(',')
                    received_seq_num = int(received_seq_num)
                    packet_timestamp = datetime.strptime(timestamp, "%Y-%m-%d %H:%M:%S.%f")

                    # Track last sequence number per client tag
                    last_seq_num = self.last_sequence_numbers.get(tag, 0)
                    if received_seq_num != last_seq_num + 1 and last_seq_num != 0:
                        missed_count = received_seq_num - last_seq_num - 1
                        print(f"[DEBUG] Missed {missed_count} packets from tag '{tag}' between {last_seq_num} and {received_seq_num}")
                        
                        missed_log = (
                            f"Missed MISSEDCNT={missed_count} packet(s)  TAG='{tag}' SRC_IP_PORT={addr} MISS_TIMESTAMP={packet_timestamp} "
                            f"between sequence LSEQNO={last_seq_num} and RSEQNO={received_seq_num} "
                        )
                        self.logger(missed_log, level="ERROR")

                    # Log the received packet
                    received_log = f"Received SEQNO={received_seq_num} TAG='{tag}' SRC_IP_PORT={addr} RCVD_TIMESTAMP={timestamp}"
                    #print(f"[DEBUG] {received_log}")
                    self.logger(received_log, level="INFO")

                    # Update last sequence number for this client tag
                    self.last_sequence_numbers[tag] = received_seq_num

                except socket.timeout:
                    #print("[DEBUG] Socket timed out waiting for UDP packet")
                    continue
                except OSError as e:
                    #print(f"[DEBUG] UDP server error: {e}")
                    self.logger(f"UDP server error: {e}", level="ERROR")
                    break
        finally:
            if self.udp_socket:
                self.udp_socket.close()
                #print("[DEBUG] UDP server socket closed.")
                self.logger("UDP server shut down.", level="INFO")
