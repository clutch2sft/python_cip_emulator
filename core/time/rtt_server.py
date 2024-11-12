# server.py
import socket

def start_server(host='127.0.0.1', port=65432):
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as server_socket:
        server_socket.bind((host, port))
        print(f"Server started on {host}:{port}, waiting for messages...")

        while True:
            data, address = server_socket.recvfrom(1024)
            print(f"Received message from {address}: {data.decode()}")
            # Send the same data back to the client
            server_socket.sendto(data, address)

if __name__ == "__main__":
    start_server()
