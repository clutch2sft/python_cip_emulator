# server.py
import socket
import signal
import sys

# Global variable for the server socket so it can be closed gracefully
server_socket = None

def signal_handler(sig, frame):
    print("\nServer is shutting down...")
    if server_socket:
        server_socket.close()  # Close the server socket
    sys.exit(0)  # Exit the program

def start_server(host='127.0.0.1', port=65432):
    global server_socket
    # Set up the signal handler for graceful shutdown
    signal.signal(signal.SIGINT, signal_handler)
    
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    server_socket.bind((host, port))
    print(f"Server started on {host}:{port}, waiting for messages...")

    try:
        while True:
            data, address = server_socket.recvfrom(1024)
            print(f"Received message from {address}: {data.decode()}")
            # Send the same data back to the client
            server_socket.sendto(data, address)

    except Exception as e:
        print(f"An error occurred: {e}")

    finally:
        # Clean up the socket in case of other exceptions
        if server_socket:
            server_socket.close()

if __name__ == "__main__":
    start_server()
