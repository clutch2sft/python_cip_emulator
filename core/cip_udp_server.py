import asyncio
import socket
from datetime import datetime
import statistics

class CIPUdpServer:
    def __init__(self, logger, consumer_config, processing_queue):
        self.logger = logger
        self.consumer_config = consumer_config
        self.processing_queue = processing_queue
        self.udp_socket = None
        self.running = False
        self.flight_times = []
        self.max_flight_time_samples = 1000
        self.flight_time_min_samples = 100
        self.flight_time_outlier_filter_multiplier = 4

    async def start(self):
        """Start the UDP server."""
        self.logger.info("CIPUdpServer: Initializing UDP server...")
        self.udp_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.udp_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.udp_socket.bind(("", self.consumer_config.get("udp_port", 2222)))
        self.logger.info("CIPUdpServer: UDP server listening on port 2222.")
        
        loop = asyncio.get_running_loop()
        self.running = True
        while self.running:
            try:
                data, addr = await loop.run_in_executor(None, self.udp_socket.recvfrom, 2048)
                await self.processing_queue.put((data, addr, datetime.now()))
            except asyncio.CancelledError:
                break
            except Exception as e:
                self.logger.error(f"CIPUdpServer: Error in UDP server loop: {e}")
        self.stop()

    async def process_packets(self):
        """Process packets from the queue."""
        self.logger.info("CIPUdpServer: Starting packet processing...")
        while self.running or not self.processing_queue.empty():
            try:
                data, addr, rcvd_timestamp = await self.processing_queue.get()
                await self.handle_packet(data, addr, rcvd_timestamp)
            except asyncio.CancelledError:
                break

    async def handle_packet(self, data, addr, rcvd_timestamp):
        """Handle a single UDP packet."""
        try:
            raw_data = data.decode().strip()
            tag, received_seq_num, sent_timestamp = raw_data.split(',')
            received_seq_num = int(received_seq_num)
            packet_timestamp = datetime.strptime(sent_timestamp.strip(), "%Y-%m-%d %H:%M:%S.%f")

            flight_time_ms = (rcvd_timestamp - packet_timestamp).total_seconds() * 1000
            is_outlier, mean, stdev = self._check_flight_time_outlier(flight_time_ms)

            log_message = f"Packet from {addr}: SEQ={received_seq_num}, TAG={tag}, FLIGHT_TIME={flight_time_ms:.3f}ms"
            if is_outlier:
                log_message += f" [OUTLIER - Mean: {mean:.3f}ms, StDev: {stdev:.3f}ms]"
                self.logger.error(log_message)
            else:
                self.logger.info(log_message)
        except Exception as e:
            self.logger.error(f"CIPUdpServer: Error handling packet: {e}")

    def _check_flight_time_outlier(self, flight_time_ms):
        """Check if a flight time is an outlier."""
        if len(self.flight_times) >= self.flight_time_min_samples:
            mean = statistics.mean(self.flight_times)
            stdev = statistics.stdev(self.flight_times)
            threshold = mean + self.flight_time_outlier_filter_multiplier * stdev
            is_outlier = flight_time_ms > threshold

            if not is_outlier:
                self.flight_times.append(flight_time_ms)
                if len(self.flight_times) > self.max_flight_time_samples:
                    self.flight_times.pop(0)

            return is_outlier, mean, stdev
        self.flight_times.append(flight_time_ms)
        return False, None, None

    def stop(self):
        """Stop the UDP server."""
        self.running = False
        if self.udp_socket:
            self.udp_socket.close()
        self.logger.info("CIPUdpServer: UDP server stopped.")