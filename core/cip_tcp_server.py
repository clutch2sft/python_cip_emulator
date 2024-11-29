import asyncio
import socket
import time


class CIPTcpServer:
    def __init__(self, logger, consumer_config, thread_manager, timestamp_queue, connections, tcp_connected_event):
        """
        Initialize the CIPTcpServer.

        Args:
            logger: Logger instance for logging.
            consumer_config: Configuration for the TCP server.
            thread_manager: Thread manager for handling tasks.
            timestamp_queue: Queue for handling timestamp requests.
            connections: Dictionary to track active connections.
            tcp_connected_event: asyncio.Event to signal active connections.
        """
        self.logger = logger
        self.consumer_config = consumer_config
        self.thread_manager = thread_manager
        self.timestamp_queue = timestamp_queue
        self.connections = connections
        self.tcp_connected_event = tcp_connected_event  # Shared event
        self.tcp_socket = None
        self.running = False
        self.consumer_ready_clients = set()  # Track clients that sent "consumer_ready"

    async def start(self):
        """Start the TCP server."""
        self.logger.info("CIPTcpServer: Initializing TCP server...")
        self.tcp_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.tcp_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.tcp_socket.bind(("", self.consumer_config.get("tcp_port", 1502)))
        self.tcp_socket.listen(5)
        self.tcp_socket.setblocking(False)
        self.logger.info("CIPTcpServer: TCP server listening on port 1502.")

        loop = asyncio.get_running_loop()
        self.running = True
        while self.running:
            try:
                conn, addr = await loop.sock_accept(self.tcp_socket)
                self.logger.info(f"CIPTcpServer: TCP connection established with {addr}")
                self.connections[addr] = conn
                asyncio.run_coroutine_threadsafe(
                    self.handle_client(conn, addr), self.thread_manager.loop
                )
            except asyncio.CancelledError:
                break
        self.stop()

    async def handle_client(self, conn, addr):
        """Handle a single TCP client."""
        conn.settimeout(0.5)
        try:
            while self.running:
                try:
                    data = await asyncio.get_running_loop().run_in_executor(None, conn.recv, 1024)
                    if data:
                        message = data.decode().strip()
                        self.logger.info(f"CIPTcpServer: Received message from {addr}: {message}")

                        # Handle specific messages
                        if message.startswith("RequestTimestamp:"):
                            try:
                                # Extract the packet number from the message
                                _, packet_number = message.split(":")
                                packet_number = int(packet_number.strip())
                                # Add the packet number and timestamp to the queue
                                await self.timestamp_queue.put((addr, packet_number, time.time_ns()))
                            except ValueError:
                                self.logger.error(f"CIPTcpServer: Malformed RequestTimestamp message: {message}")

                        elif message == "consumer_ready":
                            self.logger.info(f"CIPTcpServer: Received 'consumer_ready' from {addr}")
                            self.consumer_ready_clients.add(addr)
                            self._update_tcp_connected_event()
                        else:
                            self.logger.warning(f"CIPTcpServer: Unknown message from {addr}: {message}")
                    else:
                        self.logger.info(f"CIPTcpServer: Client {addr} disconnected.")
                        break
                except (socket.timeout, ConnectionResetError):
                    break
                except Exception as e:
                    self.logger.error(f"CIPTcpServer: Error handling client {addr}: {e}")
                    break
        except asyncio.CancelledError:
            self.logger.info(f"CIPTcpServer: Client handler cancelled for {addr}.")
        finally:
            conn.close()
            self.connections.pop(addr, None)
            self.consumer_ready_clients.discard(addr)  # Remove from ready clients
            self._update_tcp_connected_event()

    async def send_response(self, addr, message):
        """Send a response message to the specified client."""
        conn = self.connections.get(addr)
        if conn:
            try:
                await asyncio.get_running_loop().run_in_executor(None, conn.sendall, message.encode())
                self.logger.info(f"CIPTcpServer: Sent message to {addr}: {message}")
            except Exception as e:
                self.logger.error(f"CIPTcpServer: Error sending message to {addr}: {e}")
                # Close and clean up the connection if it fails
                conn.close()
                self.connections.pop(addr, None)
                self.consumer_ready_clients.discard(addr)  # Remove from ready clients
                self._update_tcp_connected_event()
        else:
            self.logger.warning(f"CIPTcpServer: No connection found for {addr}.")

    def _update_tcp_connected_event(self):
        """
        Update the TCP connected event based on consumer readiness.
        """
        if self.consumer_ready_clients:
            self.tcp_connected_event.set()
            self.logger.info(f"CIPTcpServer: Consumer ready clients detected. Event set.")
        else:
            self.tcp_connected_event.clear()
            self.logger.info(f"CIPTcpServer: No ready consumers. Event cleared.")

    def stop(self):
        """Stop the TCP server."""
        self.running = False
        if self.tcp_socket:
            self.tcp_socket.close()
        self.logger.info("CIPTcpServer: TCP server stopped.")
