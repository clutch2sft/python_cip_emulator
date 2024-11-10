import socket
import threading
import time
from datetime import datetime

SERVER_IP = "192.168.1.10"  # Replace with the server's IP (or localhost for testing)
TCP_PORT = 502
UDP_PORT = 1200

tcp_packet_interval = 1  # Interval in seconds for TCP packets
udp_packet_interval = 0.1  # Interval in seconds for UDP packets

# Global flag to control UDP packet flow
udp_running = threading.Event()

# Flag to enable or disable packet skipping for testing packet loss
enable_packet_skip = True  # Set to True to skip every 10th packet

# Generate unique log file name with timestamp
client_log_file = f"client_packet_log_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"

# Function for TCP CIP CM Client
def tcp_cip_cm_client():
    global udp_running

    while True:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as tcp_socket:
            try:
                tcp_socket.connect((SERVER_IP, TCP_PORT))
                print("Connected to TCP CIP CM Server")
                udp_running.set()  # Start UDP packet flow

                # Send keepalive messages at the specified interval
                while True:
                    message = f"CIP CM Client Keepalive - {datetime.now()}"
                    tcp_socket.sendall(message.encode())
                    print(f"Sent: {message}")
                    time.sleep(tcp_packet_interval)
            except (ConnectionResetError, ConnectionRefusedError):
                print("TCP Connection lost or refused. Attempting to reconnect...")
                udp_running.clear()  # Stop UDP packet flow

            # Wait before retrying to connect
            time.sleep(2)

# Function for UDP I/O data flow client with optional packet skipping
def udp_io_data_client():
    sequence_number = 1
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as udp_socket:
        while True:
            udp_running.wait()  # Wait for TCP connection to be established

            # Check if we should skip the packet to simulate packet loss
            if enable_packet_skip and sequence_number % 10 == 0:
                print(f"Skipping packet {sequence_number} to simulate packet loss")
                sequence_number += 1
                continue

            # Embed the sequence number directly in the packet payload
            timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
            message = f"{sequence_number},{timestamp}"
            udp_socket.sendto(message.encode(), (SERVER_IP, UDP_PORT))
            print(f"Sent UDP packet with sequence {sequence_number}")

            # Log each packet sent with timestamp
            log_client_packet(sequence_number, timestamp)
            sequence_number += 1
            time.sleep(udp_packet_interval)

# Function to log each packet sent by the client
def log_client_packet(sequence_number, timestamp):
    log_entry = f"Sent packet {sequence_number} at {timestamp}\n"
    with open(client_log_file, "a") as log:
        log.write(log_entry)

# Start TCP and UDP client threads
def start_clients():
    threading.Thread(target=tcp_cip_cm_client, daemon=True).start()
    threading.Thread(target=udp_io_data_client, daemon=True).start()

if __name__ == "__main__":
    start_clients()
    print("Clients are running. Press Ctrl+C to stop.")
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("Shutting down clients.")
