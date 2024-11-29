import asyncio
from datetime import datetime

class PacketHandler:
    def __init__(self, logger, tcp_server, udp_processing_queue, tcp_timestamp_queue, latency_smoother=None):
        """
        Initialize the PacketHandler.

        Args:
            logger: Logger instance for logging.
            tcp_server: Reference to the CIPTcpServer instance for sending responses.
            udp_processing_queue: Queue for UDP packets.
            tcp_timestamp_queue: Queue for TCP timestamp requests.
            outlier_detector: Optional outlier detector for UDP packets.
        """
        self.logger = logger
        self.tcp_server = tcp_server
        self.udp_processing_queue = udp_processing_queue
        self.tcp_timestamp_queue = tcp_timestamp_queue
        self.latency_smoother = latency_smoother
        self.running = False

    async def start(self):
        """Start monitoring queues for packet handling."""
        self.logger.info("PacketHandler: Starting packet handling tasks...")
        self.running = True

        # Monitor both queues concurrently
        await asyncio.gather(
            self._process_udp_packets(),
            self._tcpsrv_process_timestamp_requests()
        )

    async def _process_udp_packets(self):
        """Handle packets from the UDP processing queue."""
        while self.running or not self.udp_processing_queue.empty():
            try:
                data, addr, rcvd_timestamp = await self.udp_processing_queue.get()
                self.logger.info(f"PacketHandler: Received UDP packet from {addr}.")
                self._handle_udp_packet(data, addr, rcvd_timestamp)
            except asyncio.CancelledError:
                self.logger.info("PacketHandler: UDP packet processing stopped.")
                break
            except Exception as e:
                self.logger.error(f"PacketHandler: Error processing UDP packet: {e}")

    def _handle_udp_packet(self, data, addr, rcvd_timestamp):
        """Process a single UDP packet."""
        try:
            raw_data = data.decode().strip()
            tag, received_seq_num, sent_timestamp = raw_data.split(',')
            received_seq_num = int(received_seq_num)
            packet_timestamp = datetime.strptime(sent_timestamp.strip(), "%Y-%m-%d %H:%M:%S.%f")

            # Calculate flight time in milliseconds
            flight_time_ms = (rcvd_timestamp - packet_timestamp).total_seconds() * 1000

            # Convert flight time to nanoseconds for LatencySmoother
            flight_time_ns = flight_time_ms * 1_000_000

            if self.latency_smoother:
                # Check for outliers
                is_outlier, mean_ns, stdev_ns = self.latency_smoother.get_is_outlier(flight_time_ns)

                # Convert mean and standard deviation back to milliseconds for logging
                mean_ms = mean_ns / 1_000_000
                stdev_ms = stdev_ns / 1_000_000

                # Prepare log message
                log_message = (
                    f"UDP Packet from {addr}: SEQ={received_seq_num}, TAG={tag}, "
                    f"FLIGHT_TIME={flight_time_ms:.3f}ms"
                )
                if is_outlier:
                    log_message += f" [OUTLIER - Mean: {mean_ms:.3f}ms, StDev: {stdev_ms:.3f}ms]"
                    self.logger.error(log_message)
                else:
                    self.logger.info(log_message)
        except Exception as e:
            self.logger.error(f"PacketHandler: Error handling UDP packet: {e}")

    async def _tcpsrv_process_timestamp_requests(self):
        """Handle timestamp requests from the TCP timestamp queue."""
        while self.running or not self.tcp_timestamp_queue.empty():
            try:
                # Retrieve addr, packet_number, and timestamp_ns from the queue
                addr, packet_number, timestamp_ns = await self.tcp_timestamp_queue.get()
                self.logger.info(f"PacketHandler: Processing TCP timestamp request from {addr} with packet number {packet_number}.")

                # Construct response message with packet number and timestamp
                response_message = f"ResponseTimestamp:{packet_number},{timestamp_ns}\n"
                await self.tcp_server.send_response(addr, response_message)
            except asyncio.CancelledError:
                self.logger.info("PacketHandler: TCP timestamp processing stopped.")
                break
            except Exception as e:
                self.logger.error(f"PacketHandler: Error processing TCP timestamp request: {e}")



    def stop(self):
        """Stop the PacketHandler."""
        self.logger.info("PacketHandler: Stopping packet handling...")
        self.running = False
