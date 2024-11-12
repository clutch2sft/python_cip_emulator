# client.py
import socket
import time
import statistics

def calculate_rtt(host='172.16.5.163', port=65432, message="ping", num_pings=5, protocol='udp'):
    rtts = []  # List to store all RTT values

    # Choose socket type based on protocol
    if protocol.lower() == 'tcp':
        sock_type = socket.SOCK_STREAM
    elif protocol.lower() == 'udp':
        sock_type = socket.SOCK_DGRAM
    else:
        raise ValueError("Invalid protocol specified. Use 'tcp' or 'udp'.")

    with socket.socket(socket.AF_INET, sock_type) as client_socket:
        client_socket.settimeout(1)  # Set timeout to 1 second
        
        # For TCP, establish the connection before sending data
        if protocol.lower() == 'tcp':
            client_socket.connect((host, port))

        for i in range(num_pings):
            try:
                # Record send time
                send_time = time.time()
                
                if protocol.lower() == 'udp':
                    client_socket.sendto(message.encode(), (host, port))
                    data, _ = client_socket.recvfrom(1024)
                elif protocol.lower() == 'tcp':
                    client_socket.sendall(message.encode())
                    data = client_socket.recv(1024)

                receive_time = time.time()
                
                # Calculate RTT and add to list
                rtt = (receive_time - send_time) * 1000  # RTT in milliseconds
                rtts.append(rtt)
                print(f"Ping {i+1}: RTT = {rtt:.2f} ms")

            except socket.timeout:
                print(f"Ping {i+1}: Request timed out")
                rtts.append(None)  # Add None for timed-out pings for completeness

        # TCP-specific: Close the connection
        if protocol.lower() == 'tcp':
            client_socket.close()

    # Filter out None values (timeouts) for statistics calculation
    valid_rtts = [rtt for rtt in rtts if rtt is not None]

    if valid_rtts:
        # Calculate mean and standard deviation
        mean_rtt = statistics.mean(valid_rtts)
        std_dev_rtt = statistics.stdev(valid_rtts) if len(valid_rtts) > 1 else 0.0

        # Determine outliers (2 standard deviations from the mean)
        outliers = [rtt for rtt in valid_rtts if abs(rtt - mean_rtt) > 2 * std_dev_rtt]

        print("\nRTT Statistics:")
        print(f"Mean RTT: {mean_rtt:.2f} ms")
        print(f"Standard Deviation: {std_dev_rtt:.2f} ms")
        print(f"Number of Outliers: {len(outliers)}")
        if outliers:
            print(f"Outliers: {[f'{rtt:.2f} ms' for rtt in outliers]}")
    else:
        print("No successful pings to calculate statistics.")

if __name__ == "__main__":
    # Call with `protocol='tcp'` to test TCP, or `protocol='udp'` for UDP
    calculate_rtt(num_pings=500, protocol='tcp')
