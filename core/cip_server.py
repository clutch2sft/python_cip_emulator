import asyncio
import socket
import time
from datetime import datetime
from core.component_base import ComponentBase
from asyncio import Queue
from datetime import datetime
import socket
import asyncio
import time
import threading
import statistics
from core.component_base import ComponentBase
import concurrent.futures
import sys
import traceback

class CIPServer(ComponentBase):
    def __init__(self, logger, consumer_config, debug=False, thread_manager=None):
        super().__init__(name="CIPServer", logger=logger)
        self.consumer_config = consumer_config
        self.debug = debug
        self.running = False
        self.executor_thread = None  # Track the thread explicitly
        self.thread_manager = thread_manager  # Use shared thread manager
        self.processing_queue = Queue(maxsize=1000)
        self.timestamp_queue = Queue(maxsize=1000)
        self.tasks = []
        self.connections = {}
        self.tcp_socket = None
        self.udp_socket = None
        self.request_count = 0
        # Outlier detection attributes
        self.flight_times = []
        self.max_flight_time_samples = 1000
        self.last_sequence_numbers = {}
        self.flight_time_min_samples = 100
        self.flight_time_outlier_filter_multiplier = 4
        self.name = self.__class__.__name__

    async def async_start(self, custom_name="Cip_Start_Wrapper"):
        """Async wrapper to start the CIP server."""
        def wrapped_start():
            thread_name = f"CustomThread_{custom_name}"
            threading.current_thread().name = thread_name
            self.start()

        # Track the executor thread
        self.executor_thread = asyncio.get_event_loop().run_in_executor(None, wrapped_start)



    def start(self):
        """Start the CIP server."""
        self.running = True
        self.logger.info("CIPServer is starting...")
        self.tasks = []

        try:
            # Schedule server tasks on the shared event loop
            self.tasks.append(asyncio.run_coroutine_threadsafe(self.start_tcp_server(), self.thread_manager.loop))
            self.tasks.append(asyncio.run_coroutine_threadsafe(self.start_udp_server(), self.thread_manager.loop))
            self.tasks.append(asyncio.run_coroutine_threadsafe(self.process_udp_packets(), self.thread_manager.loop))
            self.tasks.append(asyncio.run_coroutine_threadsafe(self.process_timestamp_requests(), self.thread_manager.loop))

            self.logger.info("CIPServer: All server tasks scheduled.")
        except Exception as e:
            self.logger.error(f"CIPServer failed to start: {e}")
            # Attempt to stop tasks that have already started
            for task in self.tasks:
                if not task.done():
                    task.cancel()
            raise

    async def stop(self):
        """Gracefully stop the CIP server."""
        self.logger.info("Setting self.running to False.")
        self.running = False

        # Cancel and gather tasks
        pending_tasks = []
        for task in self.tasks:
            self.logger.info(f"Cancelling task: {task}")
            try:
                if isinstance(task, concurrent.futures.Future):
                    task.cancel()
                    if not task.done():
                        pending_tasks.append(task)
                elif asyncio.isfuture(task):
                    pending_tasks.append(task)
            except Exception as e:
                self.logger.error(f"Error cancelling task {task}: {e!r}")

        # Gather asyncio tasks
        if pending_tasks:
            try:
                self.logger.info(f"Waiting for {len(pending_tasks)} tasks to complete...")
                results = await asyncio.wait_for(
                    asyncio.gather(*pending_tasks, return_exceptions=True),
                    timeout=10  # Add a timeout to prevent indefinite hanging
                )
                for result in results:
                    if isinstance(result, Exception):
                        self.logger.error(f"Task shutdown error: {result!r}")
            except asyncio.TimeoutError:
                self.logger.warning("Timeout while waiting for tasks to complete.")
            except Exception as e:
                self.logger.error(f"Exception during task shutdown: {e!r}")

        # Stop the named thread if still running
        udp_thread_name = f"CIPServer_UDP_{id(self.udp_socket)}"
        self.stop_named_thread(udp_thread_name)

        # Close UDP socket
        if hasattr(self, 'udp_socket') and self.udp_socket:
            self.logger.info("Closing UDP socket...")
            try:
                self.udp_socket.close()
                self.logger.info("UDP socket closed.")
            except Exception as e:
                self.logger.error(f"Error closing UDP socket: {e!r}")

        self.logger.info("CIP server stopped.")

    def _force_terminate_thread(self, thread_name):
        """
        Forcibly terminates a thread. Use only if necessary and the thread doesn't terminate gracefully.
        """
        for thread in threading.enumerate():
            if thread.name == thread_name:
                # Log stack trace of the thread for debugging
                self.logger.debug(f"Stack trace for thread {thread_name}:")
                stack = sys._current_frames().get(thread.ident)
                if stack:
                    for filename, lineno, name, line in traceback.extract_stack(stack):
                        self.logger.debug(f"  File: {filename}, line {lineno}, in {name}")
                        if line:
                            self.logger.debug(f"    {line.strip()}")

                # Forceful termination (unsafe and not recommended unless necessary)
                self.logger.error(f"Thread {thread_name} could not be stopped gracefully. Consider redesigning logic.")

    async def start_tcp_server(self):
        """Start the TCP server."""
        try:
            self.logger.info("CIPServer: Initializing TCP server...")
            self.tcp_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.tcp_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            self.tcp_socket.bind(("", self.consumer_config.get("tcp_port", 1502)))
            self.tcp_socket.listen(5)
            self.tcp_socket.setblocking(False)
            self.logger.info("CIPServer: TCP server listening on port 1502.")

            loop = asyncio.get_running_loop()
            while self.running:
                try:
                    conn, addr = await loop.sock_accept(self.tcp_socket)
                    self.logger.info(f"CIPServer: TCP connection established with {addr}")
                    self.connections[addr] = conn
                    asyncio.run_coroutine_threadsafe(
                        self.handle_tcp_client(conn, addr), self.thread_manager.loop
                    )
                except asyncio.CancelledError:
                    break
        finally:
            if self.tcp_socket:
                self.tcp_socket.close()


    async def handle_tcp_client(self, conn, addr):
        """Handle a single TCP client."""
        conn.settimeout(0.5)  # Set a short timeout to handle client disconnection
        try:
            while self.running:
                try:
                    # Generate a custom thread name for this operation
                    custom_thread_name = f"CIPServer_TCP_{addr[0]}_{addr[1]}"
                    
                    # Wrap conn.recv with the custom thread wrapper
                    recv_callable = self.named_thread_wrapper(conn.recv, custom_thread_name, 1024)
                    data = await asyncio.get_running_loop().run_in_executor(None, recv_callable)

                    if data:
                        message = data.decode().strip()
                        self.logger.info(f"CIPServer: Received TCP message from {addr}: {message}")
                        if message == "RequestTimestamp":
                            # Capture the timestamp as close to request receipt as possible
                            await self.timestamp_queue.put((addr, time.time_ns()))
                    else:
                        # If data is empty, the client likely disconnected
                        self.logger.info(f"CIPServer: Client at {addr} disconnected.")
                        break
                except socket.timeout:
                    # Handle timeouts to keep the loop alive
                    continue
                except ConnectionResetError:
                    # Handle client disconnect or reset
                    self.logger.warning(f"CIPServer: Connection reset by client {addr}.")
                    break
                except Exception as e:
                    self.logger.error(f"CIPServer: Error handling TCP client {addr}: {e}")
                    break
        except asyncio.CancelledError:
            self.logger.info(f"CIPServer: TCP client handler cancelled for {addr}.")
        finally:
            # Ensure the connection is closed and resources are cleaned up
            try:
                conn.close()
                self.logger.info(f"CIPServer: TCP connection with {addr} closed.")
            except Exception as e:
                self.logger.error(f"CIPServer: Error closing TCP connection with {addr}: {e}")
            finally:
                # Remove the client from connections mapping
                self.connections.pop(addr, None)


    def named_thread_wrapper(self, func, thread_name, *args, **kwargs):
        """
        Wrapper to name the thread before executing a callable.
        This ensures better traceability in multithreaded environments.
        """
        def wrapped():
            current_thread = threading.current_thread()
            original_name = current_thread.name
            try:
                current_thread.name = thread_name  # Set the custom thread name
                return func(*args, **kwargs)      # Call the original function
            finally:
                current_thread.name = original_name  # Restore the original thread name
        return wrapped

    def stop_named_thread(self, thread_name):
        """
        Stops a named thread explicitly.
        """
        for thread in threading.enumerate():
            if thread.name == thread_name:
                self.logger.info(f"Attempting to stop thread: {thread_name}")

                # If the thread has a custom mechanism for stopping, use it
                if hasattr(thread, 'stop'):
                    self.logger.info(f"Calling stop on thread: {thread_name}")
                    try:
                        thread.stop()
                    except Exception as e:
                        self.logger.error(f"Error while stopping thread {thread_name}: {e!r}")
                else:
                    self.logger.warning(f"Thread {thread_name} does not support explicit stop.")

                # As a last resort, check if the thread is hanging and log debug info
                if thread.is_alive():
                    self.logger.warning(f"Thread {thread_name} is still alive. Forcing termination...")
                    self._force_terminate_thread(thread_name)

    async def start_udp_server(self):
        """Start the UDP server."""
        self.logger.info("CIPServer: Initializing UDP server...")
        self.udp_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.udp_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.udp_socket.bind(("", self.consumer_config.get("udp_port", 2222)))
        self.logger.info("CIPServer: UDP server listening on port 2222.")

        loop = asyncio.get_running_loop()
        while self.running:
            try:
                # Generate a dynamic thread name
                try:
                    custom_thread_name = f"CIPServer_UDP_{id(self.udp_socket)}"
                except Exception:
                    custom_thread_name = "CIPServer_UDP_Unknown"

                # Wrap recvfrom with the custom thread wrapper
                recvfrom_callable = self.named_thread_wrapper(
                    self.udp_socket.recvfrom,
                    custom_thread_name,
                    self.consumer_config.get("buffer_size", 2048)
                )
                data, addr = await loop.run_in_executor(None, recvfrom_callable)

                print(f"ln 152 UDP Socket in thread: {threading.current_thread().name}")
                rcvd_timestamp = datetime.now()
                await self.processing_queue.put((data, addr, rcvd_timestamp))
            except asyncio.CancelledError:
                self.logger.info("CIPServer: UDP server shutting down.")
                break
            except Exception as e:
                self.logger.error(f"CIPServer: Error in UDP server loop: {e}")

    async def process_udp_packets(self):
        """Process packets from the queue."""
        self.logger.info("CIPServer: Starting packet processing...")
        while self.running or not self.processing_queue.empty():
            try:
                data, addr, rcvd_timestamp = await self.processing_queue.get()
                await self.handle_udp_packet(data, addr, rcvd_timestamp)
            except asyncio.CancelledError:
                self.logger.info("CIPServer: Packet processing task cancelled.")
                break

            
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
                self.logger.error(log_message)
            else:
                self.logger.info(log_message)
        except Exception as e:
            self.logger.error(f"CIPServer: Error handling UDP packet: {e}")

    async def process_timestamp_requests(self):
        """Process timestamp requests from the queue."""
        while self.running or not self.timestamp_queue.empty():
            try:
                addr, timestamp_ns = await self.timestamp_queue.get()
                self.request_count += 1
                conn = self.connections.get(addr)
                if conn:
                    response_message = f"Timestamp: {timestamp_ns}\n"

                    # Generate a dynamic thread name
                    try:
                        custom_thread_name = f"CIPServer_TCP_{id(conn)}"  # Custom name for TCP send threads
                    except Exception:
                        custom_thread_name = "CIPServer_TCP_Unknown"

                    # Wrap the sendall operation with a named thread wrapper
                    sendall_callable = self.named_thread_wrapper(
                        conn.sendall,
                        custom_thread_name,
                        response_message.encode()
                    )
                    await asyncio.get_running_loop().run_in_executor(None, sendall_callable)

                    print(f"ln201 Process timestamp in thread: {threading.current_thread().name}")
            except asyncio.CancelledError:
                self.logger.info("CIPServer: Timestamp processor shutting down.")
                break
            except Exception as e:
                self.logger.error(f"CIPServer: Error processing timestamp request: {e}")

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
