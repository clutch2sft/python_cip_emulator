# client.py
import socket
import time

def calculate_rtt(host='127.0.0.1', port=65432, message="ping", num_pings=5):
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as client_socket:
        client_socket.settimeout(1)  # Set timeout to 1 second

        for i in range(num_pings):
            try:
                # Record send time
                send_time = time.time()
                client_socket.sendto(message.encode(), (host, port))
                
                # Receive the response
                data, _ = client_socket.recvfrom(1024)
                receive_time = time.time()
                
                # Calculate RTT
                rtt = (receive_time - send_time) * 1000  # RTT in milliseconds
                print(f"Ping {i+1}: RTT = {rtt:.2f} ms")

            except socket.timeout:
                print(f"Ping {i+1}: Request timed out")

if __name__ == "__main__":
    calculate_rtt()
