import socket
import threading
import time
from datetime import datetime, timedelta

class CIPClient:
    def __init__(self, logger, producer_config, tag, quiet = False):
        self.logger = logger
        self.config = producer_config
        self.tag = tag
        self.packet_interval_ms = self.config.get("packet_interval_ms", 100)
        self.enable_packet_skip = self.config.get("enable_packet_skip", False)
        self.server_ip = self.config.get("ip", "127.0.0.1")
        self.tcp_port = self.config.get("tcp_port", 1502)
        self.udp_dstport = self.config.get("udp_dstport", 2222)
        self.udp_srcport = self.config.get("udp_srcport", 2222)
        self.tcp_socket = None
        self.udp_socket = None
        self.sequence_number = 1
        self.running = False
        self.tcp_connected = threading.Event()
        self.quiet = quiet

        # Retrieve QoS (DSCP value) from config, defaulting to 0 if not provided
        self.qos = self.config.get("qos", 0)
        if not (0 <= self.qos <= 63):
            self.logger(f"Invalid DSCP value {self.qos} provided. Setting to 0.", level="WARNING")
            self.qos = 0

    def start(self):
        """Start the client by initializing TCP and UDP threads."""
        self.sequence_number = 1  # Reset sequence number on start
        self.running = True
        threading.Thread(target=self.send_tcp_keepalive, daemon=True).start()
        threading.Thread(target=self.send_udp_packets, daemon=True).start()
        self.logger(f"CIP Client started with sequence number reset to 1.", level="INFO")

    def stop(self):
        """Stop the client and close connections gracefully."""
        self.running = False
        self.tcp_connected.clear()

        if self.tcp_socket:
            try:
                self.tcp_socket.sendall("DISCONNECT".encode())
                self.logger("Sent disconnect message to server.", level="INFO")
                self.tcp_socket.shutdown(socket.SHUT_RDWR)
                self.tcp_socket.close()
                self.logger("TCP connection closed gracefully.", level="INFO")
            except OSError as e:
                self.logger(f"Error closing TCP socket: {e}", level="ERROR")

        if self.udp_socket:
            self.udp_socket.close()
            self.logger("UDP connection closed gracefully.", level="INFO")

        self.logger("CIP Client stopped.", level="INFO")

    def send_tcp_keepalive(self):
        """Manage the TCP connection and send periodic keepalive messages."""
        while self.running:
            try:
                self.tcp_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                self.tcp_socket.connect((self.server_ip, self.tcp_port))
                self.logger("Connected to TCP CIP CM Server.", level="INFO")

                self.tcp_connected.set()

                keepalive_interval = self.packet_interval_ms * \
                                     self.config.get("tcp_keepalive_multiplier", 10) / 1000.0

                while self.running and self.tcp_connected.is_set():
                    message = f"CIP CM Client Keepalive - {datetime.now()}"
                    self.tcp_socket.sendall(message.encode())
                    self.logger(f"Sent: {message}", level="INFO")

                    try:
                        data = self.tcp_socket.recv(1024).decode()
                        if data:
                            self.logger(f"Received keepalive from server: {data}", level="INFO")
                    except OSError:
                        if not self.running:
                            break

                    time.sleep(keepalive_interval)

            except (ConnectionRefusedError, ConnectionResetError, OSError) as e:
                self.logger(f"TCP connection error: {e}. Attempting to reconnect...", level="ERROR")
                self.tcp_connected.clear()

                if self.tcp_socket:
                    try:
                        self.tcp_socket.shutdown(socket.SHUT_RDWR)
                        self.tcp_socket.close()
                    except OSError as e:
                        self.logger(f"Error shutting down TCP socket during reconnect: {e}", level="ERROR")
                time.sleep(2)

    def send_udp_packets(self):
        """Simulate a UDP-based I/O data flow, conditional on TCP connection."""
        self.udp_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.udp_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)  # Enable port reuse
        self.udp_socket.bind(("", self.udp_srcport))  # Bind to source port 2222
        tos_value = (self.qos << 2) & 0xFF
        self.udp_socket.setsockopt(socket.IPPROTO_IP, socket.IP_TOS, tos_value)

        self.logger(f"UDP socket created with DSCP value {self.qos} (0x{tos_value:X}).", level="INFO")
        
        # Retrieve desired packet size from config, default to 84 bytes if not specified
        packet_size = self.config.get("packet_size", 84)

        while self.running:
            # Wait for TCP connection
            while not self.tcp_connected.is_set() and self.running:
                time.sleep(0.1)

            if not self.running:
                break

            try:
                # Optionally skip packets based on `enable_packet_skip`
                if self.enable_packet_skip and self.sequence_number % 10 == 0:
                    self.logger(f"Skipped UDP packet with sequence {self.sequence_number}", level="INFO")
                    self.sequence_number += 1
                    continue

                
                # Construct message and apply padding if needed
                timestamp = datetime.now()
                
                # Adjust timestamp for every 11th packet to create an outlier
                if self.enable_packet_skip and self.sequence_number % 11 == 0:
                    print(f"[DEBUG] Adjusted timestamp for sequence {self.sequence_number}: {timestamp}")
                    timestamp -= timedelta(seconds=2)  # Subtract 2 seconds to create an outlier
                    print(f"[DEBUG] Adjusted timestamp for sequence {self.sequence_number}: {timestamp}")
                    self.logger(f"Adjusted timestamp for sequence {self.sequence_number} to create an outlier", level="INFO")
                
                # Format the timestamp as a string after adjustment, if any
                timestamp_str = timestamp.strftime("%Y-%m-%d %H:%M:%S.%f")
                # Construct message and apply padding if needed
                message = f"{self.tag},{self.sequence_number},{timestamp_str}"
                message_bytes = message.encode()

                # Pad or truncate the message to fit the desired packet size
                if len(message_bytes) < packet_size:
                    padding = b' ' * (packet_size - len(message_bytes))
                    message_bytes += padding
                elif len(message_bytes) > packet_size:
                    self.logger(f"Warning: Packet size ({len(message_bytes)}) exceeds specified limit ({packet_size}).", level="ERROR")
                    message_bytes = message_bytes[:packet_size]

                # Send the packet with padded or truncated data
                #self.udp_socket.sendto(message_bytes, (self.server_ip, self.udp_port))
                self.udp_socket.sendto(message.encode(), (self.server_ip, self.udp_dstport))
                if not self.quiet:
                    self.logger(f"Sent UDP packet to ({self.server_ip},{self.udp_dstport}) with tag '{self.tag}', sequence {self.sequence_number}, DSCP {self.qos}, and size {len(message_bytes)} bytes", level="INFO")

                self.sequence_number += 1

                # Controlled sleep with `self.running` check
                packet_interval = self.packet_interval_ms / 1000.0
                elapsed = 0
                while elapsed < packet_interval and self.running:
                    time.sleep(0.1)
                    elapsed += 0.1

            except OSError as e:
                self.logger(f"Error in UDP client: {e}", level="ERROR")
                break

        if self.udp_socket:
            self.udp_socket.close()
            self.logger("UDP socket closed.", level="INFO")
