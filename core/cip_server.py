import asyncio
import socket
from datetime import datetime

class CIPServer:
    def __init__(self, logger, consumer_config, network_latency_ns=30000000, debug=False):
        self.logger = logger
        self.consumer_config = consumer_config
        self.running = False
        self.debug = debug
        self.last_sequence_numbers = {}
        self.class_name = self.__class__.__name__
        self.tcp_socket = None
        self.udp_socket = None
        self.processing_queue = asyncio.Queue(maxsize=1000)  # Adjust for UDP packet burst handling

    async def start(self):
        """Start the TCP and UDP servers."""
        self.running = True
        self.logger(f"{self.class_name}: CIP Server is starting...", level="INFO")

        try:
            # Start UDP and TCP handlers concurrently with packet processing
            await asyncio.gather(
                self.start_udp_server(),
                self.start_tcp_server(),
                self.process_udp_packets(),  # Add packet processing
            )
        except asyncio.CancelledError:
            self.logger(f"{self.class_name}: Server is shutting down...", level="INFO")
        except Exception as e:
            self.logger(f"{self.class_name}: Error starting server: {e}", level="ERROR")
        finally:
            await self.stop()

    async def stop(self):
        """Stop the CIP Server."""
        self.running = False
        if self.tcp_socket:
            self.tcp_socket.close()
            self.tcp_socket = None
        if self.udp_socket:
            self.udp_socket.close()
            self.udp_socket = None
        self.logger(f"{self.class_name}: CIP Server stopped.", level="INFO")

    async def start_tcp_server(self):
        """Start the TCP server for connection management."""
        self.logger(f"{self.class_name}: Initializing TCP socket...", level="INFO")
        self.tcp_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.tcp_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.tcp_socket.bind(("", self.consumer_config.get("tcp_port", 1502)))
        self.tcp_socket.listen(5)
        self.tcp_socket.setblocking(False)
        self.logger(f"{self.class_name}: TCP server listening on port {self.consumer_config.get('tcp_port', 1502)}.", level="INFO")

        loop = asyncio.get_running_loop()

        while self.running:
            try:
                # Non-blocking accept call
                conn, addr = await loop.run_in_executor(None, self.tcp_socket.accept)
                self.logger(f"{self.class_name}: TCP connection established with {addr}.", level="INFO")
                asyncio.create_task(self.handle_tcp_client(conn, addr))
            except BlockingIOError:
                # This is expected in non-blocking mode; avoid spamming the logs
                await asyncio.sleep(0.1)
            except Exception as e:
                self.logger(f"{self.class_name}: Error accepting TCP connection: {e}", level="ERROR")

    async def handle_tcp_client(self, conn, addr):
        """Handle a TCP client connection."""
        conn.settimeout(0.5)
        try:
            while self.running:
                try:
                    data = await asyncio.get_running_loop().run_in_executor(None, conn.recv, 1024)
                    if data == b"DISCONNECT":
                        self.logger(f"{self.class_name}: Client {addr} requested disconnect.", level="INFO")
                        break
                    elif data:
                        self.logger(f"{self.class_name}: Received TCP message from {addr}: {data.decode()}", level="INFO")
                except socket.timeout:
                    continue
        except Exception as e:
            self.logger(f"{self.class_name}: Error handling TCP client {addr}: {e}", level="ERROR")
        finally:
            conn.close()
            self.logger(f"{self.class_name}: TCP connection with {addr} closed.", level="INFO")

    async def start_udp_server(self):
        """Start the UDP server for packet handling."""
        self.logger(f"{self.class_name}: Initializing UDP socket...", level="INFO")
        self.udp_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.udp_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.udp_socket.bind(("", self.consumer_config.get("udp_port", 2222)))
        self.logger(f"{self.class_name}: UDP server listening on port {self.consumer_config.get('udp_port', 2222)}.", level="INFO")

        loop = asyncio.get_running_loop()

        try:
            while self.running:
                try:
                    # Receive packets non-blockingly
                    data, addr = await loop.run_in_executor(
                        None, self.udp_socket.recvfrom, self.consumer_config.get("buffer_size", 2048)
                    )
                    rcvd_timestamp = datetime.now()
                    if self.debug:
                        self.logger(f"{self.class_name}: Packet received from {addr}: {data}", level="DEBUG")
                    await self.processing_queue.put((data, addr, rcvd_timestamp))
                except Exception as e:
                    self.logger(f"{self.class_name}: Error receiving UDP packet: {e}", level="ERROR")
        except asyncio.CancelledError:
            self.logger(f"{self.class_name}: UDP server shutting down...", level="INFO")
        finally:
            self.udp_socket.close()

    async def process_udp_packets(self):
        """Process UDP packets from the queue."""
        while self.running:
            try:
                data, addr, rcvd_timestamp = await self.processing_queue.get()
                if self.debug:
                    self.logger(f"{self.class_name}: Processing packet from {addr}: {data}", level="DEBUG")
                await self.handle_udp_packet(data, addr, rcvd_timestamp)
            except Exception as e:
                self.logger(f"{self.class_name}: Error processing UDP packet: {e}", level="ERROR")

    async def handle_udp_packet(self, data, addr, rcvd_timestamp):
        """Handle a single UDP packet."""
        try:
            raw_data = data.decode().strip()
            if self.debug:
                self.logger(f"{self.class_name}: Decoded packet: {raw_data}", level="DEBUG")

            tag, received_seq_num, sent_timestamp = raw_data.split(',')
            received_seq_num = int(received_seq_num)
            sent_timestamp = sent_timestamp.strip()
            packet_timestamp = datetime.strptime(sent_timestamp, "%Y-%m-%d %H:%M:%S.%f")

            # Calculate flight time
            flight_time_ms = (rcvd_timestamp - packet_timestamp).total_seconds() * 1000
            log_msg = (
                f"Received SEQNO={received_seq_num} TAG='{tag}' SRC_IP_PORT={addr} "
                f"FLIGHT_TIME={flight_time_ms:.3f}ms"
            )
            self.logger(log_msg, level="INFO")

        except ValueError as e:
            self.logger(f"{self.class_name}: Error parsing UDP packet: {e}. Data: {data.decode()}", level="ERROR")
