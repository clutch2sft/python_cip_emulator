import asyncio
import socket
from datetime import datetime

class CIPServer:
    def __init__(self, logger, consumer_config, debug=False):
        self.logger = logger
        self.consumer_config = consumer_config
        self.debug = debug
        self.running = False
        self.tcp_socket = None
        self.udp_socket = None
        self.processing_queue = asyncio.Queue(maxsize=1000)

    async def start(self):
        """Start the CIP server."""
        self.running = True
        self.logger(f"{self.__class__.__name__}: CIP Server is starting...", level="INFO")
        try:
            await asyncio.gather(
                self.start_udp_server(),
                self.start_tcp_server(),
                self.process_udp_packets(),
            )
        except asyncio.CancelledError:
            self.logger(f"{self.__class__.__name__}: CIP Server shutdown initiated.", level="INFO")
            raise  # Ensure cancellation propagates
        except Exception as e:
            self.logger(f"{self.__class__.__name__}: CIP Server failed: {e}", level="ERROR")
            raise  # Propagate exceptions for debugging
        finally:
            await self.stop()

    async def stop(self):
        """Stop the CIP server."""
        if not self.running:
            return  # Avoid redundant shutdown attempts

        self.running = False
        if self.tcp_socket:
            self.tcp_socket.close()
            self.tcp_socket = None
        if self.udp_socket:
            self.udp_socket.close()
            self.udp_socket = None

        self.logger(f"{self.__class__.__name__}: CIP Server stopped.", level="INFO")

    async def start_tcp_server(self):
        """Start the TCP server."""
        self.logger(f"{self.__class__.__name__}: Initializing TCP server...", level="INFO")
        self.tcp_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.tcp_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.tcp_socket.bind(("", self.consumer_config.get("tcp_port", 1502)))
        self.tcp_socket.listen(5)
        self.tcp_socket.setblocking(False)
        self.logger(f"{self.__class__.__name__}: TCP server listening on port 1502.", level="INFO")

        loop = asyncio.get_running_loop()

        while self.running:
            try:
                conn, addr = await loop.run_in_executor(None, self.tcp_socket.accept)
                self.logger(f"{self.__class__.__name__}: TCP connection established with {addr}", level="INFO")
                asyncio.create_task(self.handle_tcp_client(conn, addr))
            except asyncio.CancelledError:
                self.logger(f"{self.__class__.__name__}: TCP server cancelled.", level="INFO")
                break  # Exit the loop on cancellation
            except Exception as e:
                self.logger(f"Error accepting TCP connection: {e}", level="ERROR")

        async def handle_tcp_client(self, conn, addr):
            """Handle a single TCP client."""
            conn.settimeout(0.5)
            try:
                while self.running:
                    try:
                        data = await asyncio.get_running_loop().run_in_executor(None, conn.recv, 1024)
                        if data:
                            self.logger(f"{self.__class__.__name__}: Received TCP message from {addr}: {data.decode()}", level="INFO")
                    except socket.timeout:
                        continue
            except Exception as e:
                self.logger(f"Error handling TCP client {addr}: {e}", level="ERROR")
            finally:
                conn.close()
                self.logger(f"{self.__class__.__name__}: TCP connection with {addr} closed.", level="INFO")

    async def start_udp_server(self):
        """Start the UDP server."""
        self.logger(f"{self.__class__.__name__}: Initializing UDP server...", level="INFO")
        try:
            self.udp_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            self.udp_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            self.udp_socket.bind(("", self.consumer_config.get("udp_port", 2222)))
            self.logger(f"{self.__class__.__name__}: UDP server listening on port 2222.", level="INFO")
        except Exception as e:
            self.logger(f"{self.__class__.__name__}: Failed to initialize UDP server: {e}", level="ERROR")
            raise

        loop = asyncio.get_running_loop()

        while self.running:
            try:
                data, addr = await loop.run_in_executor(None, self.udp_socket.recvfrom, self.consumer_config.get("buffer_size", 2048))
                rcvd_timestamp = datetime.now()
                if self.debug:
                    self.logger(f"{self.__class__.__name__}: Packet received from {addr}: {data}", level="DEBUG")
                await self.processing_queue.put((data, addr, rcvd_timestamp))
            except asyncio.CancelledError:
                self.logger(f"{self.__class__.__name__}: UDP server cancelled.", level="INFO")
                break  # Exit the loop on cancellation
            except Exception as e:
                self.logger(f"{self.__class__.__name__}: Error receiving UDP packet: {e}", level="ERROR")

    async def process_udp_packets(self):
        """Process packets from the queue."""
        while self.running:
            try:
                data, addr, rcvd_timestamp = await self.processing_queue.get()
                asyncio.create_task(self.handle_udp_packet(data, addr, rcvd_timestamp))
            except asyncio.CancelledError:
                self.logger(f"{self.__class__.__name__}: Packet processing cancelled.", level="INFO")
                break  # Exit the loop on cancellation
            except Exception as e:
                self.logger(f"{self.__class__.__name__}: Error processing UDP packet: {e}", level="ERROR")

    async def handle_udp_packet(self, data, addr, rcvd_timestamp):
        """Handle a single UDP packet."""
        try:
            raw_data = data.decode().strip()
            if self.debug:
                self.logger(f"{self.__class__.__name__}: Processing packet: {raw_data}", level="DEBUG")

            tag, received_seq_num, sent_timestamp = raw_data.split(',')
            received_seq_num = int(received_seq_num)
            packet_timestamp = datetime.strptime(sent_timestamp.strip(), "%Y-%m-%d %H:%M:%S.%f")

            flight_time_ms = (rcvd_timestamp - packet_timestamp).total_seconds() * 1000
            self.logger(f"{self.__class__.__name__}: Packet from {addr}: SEQ={received_seq_num}, TAG={tag}, FLIGHT_TIME={flight_time_ms:.3f}ms", level="INFO")
        except Exception as e:
            self.logger(f"{self.__class__.__name__}: Error handling UDP packet: {e}", level="ERROR")
