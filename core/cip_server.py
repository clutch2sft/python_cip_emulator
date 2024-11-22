import asyncio
import socket
from datetime import datetime
import statistics
import threading


class CIPServer:
    def __init__(self, logger, consumer_config, debug=False):
        self.logger = logger
        self.consumer_config = consumer_config
        self.debug = debug
        self.running = False
        self.tcp_socket = None
        self.udp_socket = None
        self.processing_queue = asyncio.Queue(maxsize=1000)
        self.tasks = []  # Track running tasks for cancellation
        # Outlier detection attributes
        self.flight_times = []
        self.max_flight_time_samples = 1000
        self.last_sequence_numbers = {}
        self.flight_time_min_samples = 100
        self.flight_time_outlier_filter_multiplier = 4
        self.name = self.__class__.__name__

        # Name the main thread for this server instance
        threading.current_thread().name = f"{self.name}_MainThread"

    async def start(self):
        """Start the CIP server."""
        self.running = True
        self.logger(f"{self.name}: CIP Server is starting...", level="INFO")

        try:
            # Start tasks as background tasks
            self.tasks = [
                asyncio.create_task(self.start_udp_server()),
                asyncio.create_task(self.start_tcp_server()),
                asyncio.create_task(self.process_udp_packets()),
            ]
            self.logger(f"{self.name}: All server tasks started.", level="INFO")
        except asyncio.CancelledError:
            self.logger(f"{self.name}: CIP Server received cancellation. Shutting down...", level="INFO")
        except Exception as e:
            self.logger(f"{self.name}: CIP Server failed: {e}", level="ERROR")
            raise  # Re-raise exception to propagate to the caller

    async def stop(self):
        """Stop the CIP server."""
        self.running = False
        self.logger(f"{self.name}: Stopping CIP Server...", level="INFO")
        for task in self.tasks:
            task.cancel()
        await asyncio.gather(*self.tasks, return_exceptions=True)
        self.logger(f"{self.name}: All tasks stopped.", level="INFO")

    # async def start(self):
    #     """Start the CIP server."""
    #     self.running = True
    #     self.logger(f"{self.name}: CIP Server is starting...", level="INFO")

    #     try:
    #         # Start tasks
    #         self.tasks = [
    #             asyncio.create_task(self.start_udp_server()),
    #             asyncio.create_task(self.start_tcp_server()),
    #             asyncio.create_task(self.process_udp_packets()),
    #         ]
    #         await asyncio.gather(*self.tasks)
    #     except asyncio.CancelledError:
    #         self.logger(f"{self.name}: CIP Server received cancellation. Shutting down...", level="INFO")
    #     except Exception as e:
    #         self.logger(f"{self.name}: CIP Server failed: {e}", level="ERROR")
    #     finally:
    #         await self.stop()

    # async def stop(self):
    #     """Stop the CIP server."""
    #     self.logger(f"{self.name}: Stopping CIP Server...", level="INFO")
    #     self.running = False

    #     # Cancel running tasks
    #     for task in self.tasks:
    #         task.cancel()
    #     await asyncio.gather(*self.tasks, return_exceptions=True)

    #     # Close sockets
    #     if self.tcp_socket:
    #         self.tcp_socket.close()
    #         self.tcp_socket = None
    #     if self.udp_socket:
    #         self.udp_socket.close()
    #         self.udp_socket = None

    #     self.logger(f"{self.name}: CIP Server stopped.", level="INFO")

    async def start_tcp_server(self):
        """Start the TCP server."""
        self.logger(f"{self.name}: Initializing TCP server...", level="INFO")
        thread_name = f"{self.name}_TCP_ServerThread"
        threading.current_thread().name = thread_name

        self.tcp_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.tcp_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.tcp_socket.bind(("", self.consumer_config.get("tcp_port", 1502)))
        self.tcp_socket.listen(5)
        self.tcp_socket.setblocking(False)
        self.logger(f"{self.name}: TCP server listening on port 1502.", level="INFO")

        loop = asyncio.get_running_loop()

        while self.running:
            try:
                conn, addr = await loop.sock_accept(self.tcp_socket)
                self.logger(f"{self.name}: TCP connection established with {addr}", level="INFO")
                asyncio.create_task(self.handle_tcp_client(conn, addr))
            except asyncio.CancelledError:
                self.logger(f"{self.name}: TCP server shutting down.", level="INFO")
                break
            except Exception as e:
                self.logger(f"{self.name}: Error accepting TCP connection: {e}", level="ERROR")

    async def handle_tcp_client(self, conn, addr):
        """Handle a single TCP client."""
        conn.settimeout(0.5)
        try:
            while self.running:
                try:
                    data = await asyncio.get_running_loop().run_in_executor(None, conn.recv, 1024)
                    if data:
                        self.logger(f"{self.name}: Received TCP message from {addr}: {data.decode()}", level="INFO")
                except socket.timeout:
                    continue
        except asyncio.CancelledError:
            self.logger(f"{self.name}: TCP client handler cancelled for {addr}.", level="INFO")
        except Exception as e:
            self.logger(f"{self.name}: Error handling TCP client {addr}: {e}", level="ERROR")
        finally:
            conn.close()
            self.logger(f"{self.name}: TCP connection with {addr} closed.", level="INFO")

    async def start_udp_server(self):
        """Start the UDP server."""
        self.logger(f"{self.name}: Initializing UDP server...", level="INFO")
        thread_name = f"{self.name}_UDP_ServerThread"
        threading.current_thread().name = thread_name

        try:
            self.udp_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            self.udp_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            self.udp_socket.bind(("", self.consumer_config.get("udp_port", 2222)))
            self.logger(f"{self.name}: UDP server listening on port 2222.", level="INFO")
        except Exception as e:
            self.logger(f"{self.name}: Failed to initialize UDP server: {e}", level="ERROR")
            raise

        loop = asyncio.get_running_loop()

        while self.running:
            try:
                data, addr = await loop.run_in_executor(
                    None, self.udp_socket.recvfrom, self.consumer_config.get("buffer_size", 2048)
                )
                rcvd_timestamp = datetime.now()
                await self.processing_queue.put((data, addr, rcvd_timestamp))
            except asyncio.CancelledError:
                self.logger(f"{self.name}: UDP server shutting down.", level="INFO")
                break
            except Exception as e:
                self.logger(f"{self.name}: Error receiving UDP packet: {e}", level="ERROR")

    async def process_udp_packets(self):
        """Process packets from the queue."""
        while self.running:
            try:
                data, addr, rcvd_timestamp = await self.processing_queue.get()
                asyncio.create_task(self.handle_udp_packet(data, addr, rcvd_timestamp))
            except asyncio.CancelledError:
                self.logger(f"{self.name}: UDP packet processor shutting down.", level="INFO")
                break
            except Exception as e:
                self.logger(f"{self.name}: Error processing UDP packet: {e}", level="ERROR")

    async def handle_udp_packet(self, data, addr, rcvd_timestamp):
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
                self.logger(log_message, level="ERROR")
            else:
                self.logger(log_message, level="INFO")
        except Exception as e:
            self.logger(f"{self.name}: Error handling UDP packet: {e}", level="ERROR")

    def _check_flight_time_outlier(self, flight_time_ms):
        """Check if a flight time is an outlier."""
        if flight_time_ms < 0:
            return False, None, None

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
        if len(self.flight_times) > self.max_flight_time_samples:
            self.flight_times.pop(0)

        return False, None, None
