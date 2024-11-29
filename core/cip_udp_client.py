import socket
import asyncio
from datetime import datetime, timedelta


class CIPUdpClient:
    def __init__(self, server_ip, udp_dstport, udp_srcport, logger, drift_corrector, tcp_connected_event, qos=0, packet_interval_ms=100, enable_packet_skip=False, tag="default", debug=False):
        self.server_ip = server_ip
        self.udp_dstport = udp_dstport
        self.udp_srcport = udp_srcport
        self.logger = logger
        self.drift_corrector = drift_corrector
        self.tcp_connected_event = tcp_connected_event  # Shared event from TCP server
        self.qos = qos
        self.packet_interval_ms = packet_interval_ms
        self.enable_packet_skip = enable_packet_skip
        self.tag = tag
        self.sequence_number = 1
        self.running = False
        self.debug = debug

        # Validate DSCP value
        if not (0 <= self.qos <= 63):
            self.logger(f"CIPUdpClient: Invalid DSCP value {self.qos}. Setting to 0.", level="WARNING")
            self.qos = 0

        # Create UDP socket
        self.udp_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.udp_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.udp_socket.bind(("", self.udp_srcport))
        tos_value = (self.qos << 2) & 0xFF
        self.udp_socket.setsockopt(socket.IPPROTO_IP, socket.IP_TOS, tos_value)

    def start(self):
        """Start the UDP client."""
        self.running = True
        self.logger("CIPUdpClient: Starting UDP client.", level="INFO")

    def stop(self):
        """Stop the UDP client."""
        self.logger("CIPUdpClient: Stopping UDP client.", level="INFO")
        self.running = False
        self.udp_socket.close()
        self.logger("CIPUdpClient: UDP socket closed.", level="DEBUG")

    async def send_udp_packets(self):
        """Send UDP packets when TCP connection is active."""
        while self.running:
            # Wait for TCP connection to be established
            if not self.tcp_connected_event.is_set():
                if self.debug:
                    self.logger("CIPUdpClient: Waiting for TCP connection...", level="DEBUG")
                await asyncio.sleep(0.1)
                continue  # Check again after the wait

            if not self.running:
                break

            try:
                timestamp = datetime.now()
                clock_diff = self.drift_corrector.get_drift()
                time_adjustment = timedelta(microseconds=(-clock_diff / 1000))
                adjusted_time = timestamp + time_adjustment

                # Simulate outlier packet for testing
                if self.enable_packet_skip and self.sequence_number % 11 == 0:
                    adjusted_time -= timedelta(seconds=2)
                    self.logger(f"CIPUdpClient: Adjusted timestamp for sequence {self.sequence_number} to create an outlier.", level="DEBUG")

                timestamp_str = adjusted_time.strftime("%Y-%m-%d %H:%M:%S.%f")
                message = f"{self.tag},{self.sequence_number},{timestamp_str}".encode()
                self.udp_socket.sendto(message, (self.server_ip, self.udp_dstport))

                self.logger(f"CIPUdpClient: Sent UDP packet #{self.sequence_number} to {self.server_ip}:{self.udp_dstport}.", level="DEBUG")
                self.sequence_number += 1
                await asyncio.sleep(self.packet_interval_ms / 1000.0)

            except OSError as e:
                self.logger(f"CIPUdpClient: Error sending UDP packet: {e}", level="ERROR")
                break
