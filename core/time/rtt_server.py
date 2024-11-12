# server_tcp.py
import socket
import signal
import sys

def signal_handler(sig, frame):
    print("\nServer is shutting down...")
    sys.exit(0)  # Exit the program

def start_server(host='127.0.0.1', port=65432, protocol='udp'):
    # Set up the signal handler for graceful shutdown
    signal.signal(signal.SIGINT, signal_handler)

    if protocol.lower() == 'udp':
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as server_socket:
            server_socket.bind((host, port))
            print(f"UDP server started on {host}:{port}, waiting for messages...")

            while True:
                data, address = server_socket.recvfrom(1024)
                print(f"Received message from {address}: {data.decode()}")
                server_socket.sendto(data, address)

    elif protocol.lower() == 'tcp':
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server_socket:
            server_socket.bind((host, port))
            server_socket.listen()
            print(f"TCP server started on {host}:{port}, waiting for connections...")

            while True:
                conn, address = server_socket.accept()
                with conn:
                    print(f"Connected by {address}")
                    while True:
                        data = conn.recv(1024)
                        if not data:
                            break
                        print(f"Received message: {data.decode()}")
                        conn.sendall(data)

if __name__ == "__main__":
    # Start the server with `protocol='tcp'` for TCP or `protocol='udp'` for UDP
    start_server(protocol='tcp')
