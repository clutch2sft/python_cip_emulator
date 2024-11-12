import time
import zmq

# Initialize ZeroMQ
context = zmq.Context()
socket = context.socket(zmq.PAIR)
socket.bind("tcp://172.16.5.163:5555")  # Bind to its own IP address

# Set up precise timing
start_time_ns = time.perf_counter_ns()
system_start_time_ns = time.time_ns()

def get_precise_timestamp():
    current_time_ns = time.perf_counter_ns()
    elapsed_time_ns = current_time_ns - start_time_ns
    return system_start_time_ns + elapsed_time_ns

# Periodically send timestamps to Application 2
while True:
    timestamp_ns = get_precise_timestamp()
    socket.send_pyobj(timestamp_ns)
    time.sleep(0.1)  # Send every 100ms