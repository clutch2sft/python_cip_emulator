import asyncio
import time


class CIPTcpClient:
    def __init__(self, server_ip, tcp_port, logger, ts_request_interval=0.1, ts_rint_backoff=5, response_queue=None, logger_app=None, debug=False):
        """
        Initialize the CIPTcpClient.

        Args:
            server_ip (str): Server IP address.
            tcp_port (int): Server TCP port.
            logger: Logger instance.
            ts_request_interval (float): Interval for sending timestamp requests in seconds.
            ts_rint_backoff (int): Factor to back off the request interval after stability.
            response_queue: Queue to store responses for processing.
            logger_app: Application-level logger instance.
            debug (bool): Enable debug logging.
        """
        self.server_ip = server_ip
        self.tcp_port = tcp_port
        self.logger = logger
        self.ts_request_interval = ts_request_interval
        self.ts_rint_backoff = ts_rint_backoff
        self.response_queue = response_queue  # Shared queue for responses
        self.logger_app = logger_app
        self.debug = debug

        self.running = False
        self.tcp_connected = asyncio.Event()  # Indicates TCP connection status
        self.stable_flag_sent = False
        self.packet_number = 1  # Packet number for matching responses
        self.pending_requests = {}  # Stores sent packet numbers with their send timestamps

    async def start(self):
        """Start the TCP client."""
        self.running = True
        self.logger("CIPTcpClient: Starting TCP client.", level="INFO")
        await self._run_tcp_client()

    async def stop(self):
        """Stop the TCP client."""
        self.logger("CIPTcpClient: Stopping TCP client.", level="INFO")
        self.running = False
        self.tcp_connected.clear()
        self.pending_requests.clear()  # Clear pending requests to avoid lingering data

    async def _run_tcp_client(self):
        """Handle the TCP client lifecycle."""
        while self.running:
            try:
                reader, writer = await asyncio.open_connection(self.server_ip, self.tcp_port)
                self.tcp_connected.set()
                self.logger(f"CIPTcpClient: Connected to TCP server at {self.server_ip}:{self.tcp_port}.", level="INFO")

                await asyncio.gather(
                    self._send_requests(writer),
                    self._receive_responses(reader)
                )
            except (ConnectionRefusedError, ConnectionResetError, OSError) as e:
                self.logger(f"CIPTcpClient: TCP connection error: {e}. Retrying...", level="ERROR")
                self.tcp_connected.clear()
                await asyncio.sleep(2)

    async def _send_requests(self, writer):
        """Send periodic timestamp requests and consumer_ready."""
        current_interval = self.ts_request_interval

        while self.running:
            try:
                if not self.stable_flag_sent:
                    # Prepare timestamp request
                    message = f"RequestTimestamp:{self.packet_number}"
                    send_time_ns = time.time_ns()  # Timestamp when the request is sent

                    # Store the request timestamp and packet number
                    self.pending_requests[self.packet_number] = send_time_ns

                    # Send the request
                    writer.write(message.encode())
                    await writer.drain()
                    self.logger(f"CIPTcpClient: Sent: {message}", level="DEBUG")

                    # Increment packet number
                    self.packet_number += 1

                else:
                    # Stability achieved: Send consumer_ready once
                    message = "consumer_ready"
                    writer.write(message.encode())
                    await writer.drain()
                    self.logger("CIPTcpClient: Sent: consumer_ready", level="INFO")
                    self.stable_flag_sent = True  # Ensure this is sent only once

                # Adjust interval dynamically based on stability
                if not self.stable_flag_sent and self.response_queue.get_stable_flag():
                    current_interval *= self.ts_rint_backoff
                    self.logger(f"CIPTcpClient: Stability detected. Interval backed off to {current_interval} seconds.", level="INFO")

                await asyncio.sleep(current_interval)
            except Exception as e:
                self.logger(f"CIPTcpClient: Error sending request: {e}", level="ERROR")
                break

    async def _receive_responses(self, reader):
        """Process responses from the TCP server."""
        while self.running:
            try:
                data = await asyncio.wait_for(reader.read(1024), timeout=self.ts_request_interval)
                if not data:
                    self.logger("CIPTcpClient: Server disconnected.", level="WARNING")
                    self.running = False
                    break

                message = data.decode().strip()
                self.logger(f"CIPTcpClient: Received: {message}", level="DEBUG")

                # Parse response
                if message.startswith("ResponseTimestamp:"):
                    packet_number, server_recv_time_ns = self._parse_response(message)

                    # Verify matching request
                    if packet_number in self.pending_requests:
                        send_time_ns = self.pending_requests.pop(packet_number)
                        recv_time_ns = time.time_ns()  # Timestamp when the response was received

                        # Enqueue for processing
                        if self.response_queue:
                            await self.response_queue.put((packet_number, send_time_ns, recv_time_ns, server_recv_time_ns))

                        self.logger(f"CIPTcpClient: Processed packet {packet_number}.", level="INFO")
                    else:
                        self.logger(f"CIPTcpClient: Received unmatched packet number: {packet_number}", level="WARNING")

            except asyncio.TimeoutError:
                if self.debug:
                    self.logger("CIPTcpClient: Waiting for server response...", level="DEBUG")
            except Exception as e:
                self.logger(f"CIPTcpClient: Error receiving response: {e}", level="ERROR")
                break

    def _parse_response(self, message):
        """Parse the response to extract the packet number and server receive timestamp."""
        try:
            _, packet_info = message.split(":")
            packet_number, server_recv_time_ns = map(int, packet_info.split(","))
            return packet_number, server_recv_time_ns
        except ValueError as e:
            self.logger(f"CIPTcpClient: Failed to parse response: {message}, error: {e}", level="ERROR")
            return None, None
