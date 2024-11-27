import asyncio
import socket
import time

class CIPTcpServer:
    def __init__(self, logger, consumer_config, thread_manager, timestamp_queue, connections):
        self.logger = logger
        self.consumer_config = consumer_config
        self.thread_manager = thread_manager
        self.timestamp_queue = timestamp_queue
        self.connections = connections
        self.tcp_socket = None
        self.running = False

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
                        if message == "RequestTimestamp":
                            await self.timestamp_queue.put((addr, time.time_ns()))
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
        else:
            self.logger.warning(f"CIPTcpServer: No connection found for {addr}.")
    
    def stop(self):
        """Stop the TCP server."""
        self.running = False
        if self.tcp_socket:
            self.tcp_socket.close()
        self.logger.info("CIPTcpServer: TCP server stopped.")
