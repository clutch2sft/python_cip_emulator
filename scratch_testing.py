import socket
import time
from datetime import datetime

def udp_test_client(server_ip, server_port, tag="TEST_TAG", initial_seq_num=0, count=10, delay=1.0):
    """
    Sends UDP packets to a server with the expected format.
    
    Args:
        server_ip (str): The IP address of the server.
        server_port (int): The port number of the server.
        tag (str): Tag to identify the packet.
        initial_seq_num (int): Initial sequence number.
        count (int): Number of packets to send. Use 0 for infinite.
        delay (float): Delay between sending packets (in seconds).
    """
    try:
        # Create a UDP socket
        client_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

        print(f"Sending UDP packets to {server_ip}:{server_port}")
        seq_num = initial_seq_num

        while count == 0 or seq_num < initial_seq_num + count:
            # Construct the packet
            sent_timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
            packet = f"{tag},{seq_num},{sent_timestamp}"

            try:
                # Send the UDP packet
                client_socket.sendto(packet.encode(), (server_ip, server_port))
                print(f"Sent: {packet}")
            except Exception as e:
                print(f"Error sending packet: {e}")
                break

            seq_num += 1

            # Wait for the specified delay before sending the next packet
            time.sleep(delay)

        print("Finished sending packets.")
    finally:
        client_socket.close()
        print("UDP client socket closed.")




def tcp_client(server_ip, server_port, message):
    """Connects to a TCP server, sends a message, and optionally receives a response."""
    try:
        # Create a TCP socket
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as client_socket:
            print(f"Connecting to server at {server_ip}:{server_port}...")
            client_socket.connect((server_ip, server_port))
            print("Connected to server.")

            # Send a message to the server
            print(f"Sending message: {message}")
            client_socket.sendall(message.encode('utf-8'))

            # Optionally receive a response from the server
            response = client_socket.recv(1024).decode('utf-8')
            print(f"Received response: {response}")

    except ConnectionRefusedError:
        print(f"Connection to {server_ip}:{server_port} refused. Is the server running?")
    except Exception as e:
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    # Replace these values with your server's IP and port
    SERVER_IP = "127.0.0.1"  # Localhost
    SERVER_PORT = 1502       # Port your server is listening on
    MESSAGE = "RequestTimestamp"  # Message to send to the server

    # Run the client
    tcp_client(SERVER_IP, SERVER_PORT, MESSAGE)

    # Server details
    SERVER_IP = "127.0.0.1"  # Replace with your server's IP address
    SERVER_PORT = 2222       # Replace with your server's UDP port

    # Test client settings
    TAG = "UDP_TEST"          # Tag for the packets
    INITIAL_SEQ_NUM = 0       # Starting sequence number
    PACKET_COUNT = 10         # Number of packets to send (0 for infinite)
    DELAY_BETWEEN_PACKETS = 0.5  # Delay in seconds between packets

    udp_test_client(
        server_ip=SERVER_IP,
        server_port=SERVER_PORT,
        tag=TAG,
        initial_seq_num=INITIAL_SEQ_NUM,
        count=PACKET_COUNT,
        delay=DELAY_BETWEEN_PACKETS
    )