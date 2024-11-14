# singleton_server.py
import socket
import signal
import sys
import threading

class SingletonRTTServer:
    _instance = None  # Singleton instance
    _lock = threading.Lock()  # Lock for thread-safe singleton

    def __new__(cls, *args, **kwargs):
        with cls._lock:
            if cls._instance is None:
                cls._instance = super(SingletonRTTServer, cls).__new__(cls)
        return cls._instance

    def __init__(self, host='127.0.0.1', port=65432, protocol='udp'):
        self.host = host
        self.port = port
        self.protocol = protocol.lower()
        self.server_socket = None
        self.running = False
        self.server_thread = None

    def start(self):
        if self.running:
            print("Server is already running.")
            return

        # Set up the signal handler for graceful shutdown
        signal.signal(signal.SIGINT, self._signal_handler)

        # Initialize the server socket based on the protocol
        if self.protocol == 'udp':
            self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        elif self.protocol == 'tcp':
            self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        else:
            raise ValueError("Unsupported protocol. Use 'tcp' or 'udp'.")

        self.server_socket.bind((self.host, self.port))
        self.running = True
        print(f"{self.protocol.upper()} server started on {self.host}:{self.port}")

        # Start the server loop in a separate thread
        self.server_thread = threading.Thread(target=self._run_server)
        self.server_thread.start()

    def stop(self):
        if not self.running:
            print("Server is not running.")
            return

        print("Stopping the server...")
        self.running = False
        if self.protocol == 'tcp':
            # For TCP, a dummy connection to unblock `accept`
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as temp_socket:
                temp_socket.connect((self.host, self.port))
                temp_socket.close()
        
        # Close the server socket and wait for the thread to finish
        if self.server_socket:
            self.server_socket.close()
            self.server_socket = None
        if self.server_thread:
            self.server_thread.join()
        print("Server stopped.")

    def _run_server(self):
        if self.protocol == 'udp':
            while self.running:
                try:
                    data, address = self.server_socket.recvfrom(1024)
                    print(f"Received message from {address}: {data.decode()}")
                    self.server_socket.sendto(data, address)
                except socket.error:
                    break  # Exit loop on socket close

        elif self.protocol == 'tcp':
            self.server_socket.listen()
            while self.running:
                try:
                    conn, address = self.server_socket.accept()
                    print(f"Connected by {address}")
                    with conn:
                        while self.running:
                            data = conn.recv(1024)
                            if not data:
                                break
                            print(f"Received message: {data.decode()}")
                            conn.sendall(data)
                except socket.error:
                    break  # Exit loop on socket close

    def _signal_handler(self, sig, frame):
        print("\nReceived interrupt signal, stopping server...")
        self.stop()
        sys.exit(0)

# Usage example
# if __name__ == "__main__":
#     # Create a single instance of the server
#     server = SingletonRTTServer(protocol='tcp')
#     server.start()
