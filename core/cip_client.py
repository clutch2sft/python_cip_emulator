import asyncio
import socket
from datetime import datetime, timedelta
from core.time.driftcorrection import DriftCorrectorBorg

class CIPClient:
    def __init__(self, logger, producer_config, tag, quiet=False, logger_app=None):
        self.logger = logger
        self.config = producer_config
        self.tag = tag
        self.packet_interval_ms = self.config.get("packet_interval_ms", 100)
        self.enable_packet_skip = self.config.get("enable_packet_skip", False)
        self.server_ip = self.config.get("ip", "127.0.0.1")
        self.tcp_port = self.config.get("tcp_port", 1502)
        self.udp_dstport = self.config.get("udp_dstport", 2222)
        self.udp_srcport = self.config.get("udp_srcport", 2222)
        self.sequence_number = 1
        self.running = False
        self.quiet = quiet
        self.tcp_connected = asyncio.Event()  # Changed to asyncio.Event for async compatibility

        self.qos = self.config.get("qos", 0)
        self.drift_corrector = DriftCorrectorBorg(network_latency_ns=30000000, logger_app=logger_app)
        self.class_name = self.__class__.__name__
        # Validate DSCP value (QoS)
        if not (0 <= self.qos <= 63):
            self.logger(f"Invalid DSCP value {self.qos}. Setting to 0.", level="WARNING")
            self.qos = 0

    async def start(self):
        """Start the client and run tasks for TCP and UDP communication."""
        self.sequence_number = 1  # Reset sequence number
        self.running = True
        self.logger(f"{self.class_name}: Starting CIP Client for tag '{self.tag}'.", level="INFO")

        # Run TCP keepalive and UDP packet sending concurrently
        await asyncio.gather(
            self.send_tcp_keepalive(),
            self.send_udp_packets()
        )

    async def stop(self):
        """Stop the client and close connections gracefully."""
        self.logger(f"{self.class_name}: Stopping CIP Client for tag '{self.tag}'. Average clock drift: {self.drift_corrector.get_drift()} ns.", level="INFO")
        self.running = False

    async def send_tcp_keepalive(self):
        """Establish TCP connection and send periodic keepalive messages."""
        while self.running:
            try:
                reader, writer = await asyncio.open_connection(self.server_ip, self.tcp_port)
                self.tcp_connected.set()  # Indicate that the TCP connection is active
                self.logger(f"{self.class_name}: Connected to TCP server at {self.server_ip}:{self.tcp_port}.", level="INFO")

                keepalive_interval = self.packet_interval_ms * \
                                    self.config.get("tcp_keepalive_multiplier", 10) / 1000.0

                while self.running:
                    message = f"CIP CM Client Keepalive - {datetime.now()}"
                    writer.write(message.encode())
                    await writer.drain()
                    self.logger(f"{self.class_name}: Sent TCP keepalive: {message}", level="INFO")

                    try:
                        # Read data with a timeout equal to the keepalive interval
                        data = await asyncio.wait_for(reader.read(1024), timeout=keepalive_interval)
                        self.logger(f"{self.class_name}: Received keepalive from server: {data.decode()}", level="INFO")
                    except asyncio.TimeoutError:
                        pass

                    # Sleep for the keepalive interval
                    await asyncio.sleep(keepalive_interval)

            except (ConnectionRefusedError, ConnectionResetError, OSError) as e:
                self.logger(f"{self.class_name}: TCP connection error: {e}. Retrying...", level="ERROR")
                self.tcp_connected.clear()  # Indicate that the TCP connection is inactive
                await asyncio.sleep(2)


    async def send_udp_packets(self):
        """Simulate a UDP-based I/O data flow, conditional on TCP connection."""
        self.logger(f"{self.class_name}: Starting UDP packet sender for {self.tag}.", level="INFO")
        # Create and configure the UDP socket
        self.udp_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.udp_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)  # Enable port reuse
        self.udp_socket.bind(("", self.udp_srcport))  # Bind to source port

        # Set DSCP value for QoS
        tos_value = (self.qos << 2) & 0xFF
        self.udp_socket.setsockopt(socket.IPPROTO_IP, socket.IP_TOS, tos_value)

        self.logger(f"UDP socket created with DSCP value {self.qos} (0x{tos_value:X}).", level="INFO")

        packet_size = self.config.get("packet_size", 84)  # Default packet size

        while self.running:
            # Wait for TCP connection
            self.logger(f"{self.class_name}: Waiting for TCP connection to send UDP packets.", level="DEBUG")

            while not self.tcp_connected.is_set() and self.running:
                await asyncio.sleep(0.1)
                self.logger(f"{self.class_name}: TCP connection established. Proceeding with UDP packet transmission.", level="DEBUG")
            if not self.running:
                break

            try:
                timestamp = datetime.now()
                clock_diff = self.drift_corrector.get_drift()
                microseconds_to_add = clock_diff / 1000
                time_adjustment = timedelta(microseconds=(microseconds_to_add * -1))
                adjusted_time = timestamp + time_adjustment

                # Adjust timestamp for every 11th packet to create an outlier
                if self.enable_packet_skip and self.sequence_number % 11 == 0:
                    adjusted_time -= timedelta(seconds=2)
                    self.logger(f"Adjusted timestamp for sequence {self.sequence_number} to create an outlier.", level="INFO")

                timestamp_str = adjusted_time.strftime("%Y-%m-%d %H:%M:%S.%f")
                message = f"{self.class_name}: {self.tag},{self.sequence_number},{timestamp_str}"
                message_bytes = message.encode()

                # Pad or truncate the message to fit the desired packet size
                if len(message_bytes) < packet_size:
                    message_bytes += b' ' * (packet_size - len(message_bytes))
                elif len(message_bytes) > packet_size:
                    self.logger(f"Warning: Packet size ({len(message_bytes)}) exceeds specified limit ({packet_size}).", level="ERROR")
                    message_bytes = message_bytes[:packet_size]

                # Send the packet to the server
                self.udp_socket.sendto(message_bytes, (self.server_ip, self.udp_dstport))
                self.logger(f"Sent UDP packet to ({self.server_ip},{self.udp_dstport}) with tag '{self.tag}', "
                            f"sequence {self.sequence_number}, DSCP {self.qos}, and size {len(message_bytes)} bytes.",
                            level="INFO")

                self.sequence_number += 1

                # Sleep for the interval while respecting `self.running`
                await asyncio.sleep(self.packet_interval_ms / 1000.0)

            except OSError as e:
                self.logger(f"Error in UDP client: {e}", level="ERROR")
                break

        # Close the UDP socket
        try:
            self.logger("Closing UDP socket...", level="DEBUG")
            self.udp_socket.close()
            self.logger("UDP socket closed.", level="DEBUG")
        except OSError as e:
            self.logger(f"Error closing UDP socket: {e}", level="ERROR")

    async def monitor_drift(self):
        """Periodically log the current drift for debugging."""
        while self.running:
            drift_ns = self.drift_corrector.get_drift()
            self.logger(f"{self.class_name}: Current clock drift: {drift_ns} ns.", level="DEBUG")
            await asyncio.sleep(5)  # Adjust frequency as needed
