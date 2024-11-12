import time
import zmq

# Initialize ZeroMQ
context = zmq.Context()
socket = context.socket(zmq.PAIR)
socket.connect("tcp://172.16.5.163:5555")  # Connect to Application 1's IP address

# Set up precise timing
start_time_ns = time.perf_counter_ns()
system_start_time_ns = time.time_ns()

def get_precise_timestamp():
    current_time_ns = time.perf_counter_ns()
    elapsed_time_ns = current_time_ns - start_time_ns
    return system_start_time_ns + elapsed_time_ns

# Periodically receive timestamps and calculate discrepancies
while True:
    received_timestamp_ns = socket.recv_pyobj()
    local_timestamp_ns = get_precise_timestamp()
    discrepancy_ns = abs(local_timestamp_ns - received_timestamp_ns)
    print(f"Timing Discrepancy: {discrepancy_ns} ns")
    time.sleep(0.1)