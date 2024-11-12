import time
import zmq
import threading

class TimeSyncServer:
    def __init__(self, ip_address='0.0.0.0', port=5555):
        self.ip_address = ip_address
        self.port = port
        self.context = zmq.Context()
        self.socket = self.context.socket(zmq.PAIR)
        self.socket.bind(f"tcp://{self.ip_address}:{self.port}")
        self.running = False

    def start(self):
        """Start the server and begin sending timestamps."""
        self.running = True
        threading.Thread(target=self._run, daemon=True).start()

    def stop(self):
        """Stop the server."""
        self.running = False

    def _run(self):
        while self.running:
            # Send the current timestamp to the client
            timestamp_ns = time.time_ns()
            self.socket.send_pyobj(timestamp_ns)
            print(f"Server sent timestamp: {timestamp_ns}")
            time.sleep(0.1)  # Send every 100 ms

    def close(self):
        """Close the ZeroMQ socket and context."""
        self.socket.close()
        self.context.term()
