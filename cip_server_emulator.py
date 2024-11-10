import socket
import threading
import time
import sys
import os
from datetime import datetime

# Attempt to import tkinter; if unavailable, default to headless mode
try:
    import tkinter as tk
    from tkinter.scrolledtext import ScrolledText
    GUI_AVAILABLE = True
except ImportError:
    GUI_AVAILABLE = False

TCP_PORT = 502  # Customizable TCP port for CIP CM
UDP_PORT = 1200  # Customizable UDP port for I/O data
BUFFER_SIZE = 1024

# Global variables
reset_tracking = threading.Event()
server_running = False
tcp_thread = None
udp_thread = None
is_headless = False  # Will be set to True in headless mode

# Generate unique log file name with timestamp
log_file = f"packet_log_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"

# Function to handle TCP CIP Connection Manager
def tcp_cip_cm_server(packet_interval, log_widget=None):
    global reset_tracking, server_running

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as tcp_socket:
        tcp_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        tcp_socket.bind(("", TCP_PORT))
        tcp_socket.listen()
        log_message(log_widget, "TCP CIP CM Server started, waiting for connection...")

        while server_running:
            try:
                conn, addr = tcp_socket.accept()
                log_message(log_widget, f"TCP connection established with {addr}")
                reset_tracking.set()

                while server_running:
                    message = f"CIP CM Keepalive - {datetime.now()}"
                    conn.sendall(message.encode())
                    log_message(log_widget, f"Sent: {message}")
                    time.sleep(packet_interval)
            except (ConnectionResetError, BrokenPipeError):
                log_message(log_widget, "TCP Connection closed by client.")
            finally:
                reset_tracking.clear()
                if conn:
                    conn.close()

# Function to handle UDP I/O data flow
def udp_io_data_server(log_widget=None):
    global reset_tracking, server_running
    last_sequence_number = 0  # Track the last received sequence number

    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as udp_socket:
        udp_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        udp_socket.bind(("", UDP_PORT))
        log_message(log_widget, "UDP I/O Data Server started, waiting for data...")

        while server_running:
            reset_tracking.wait()  # Wait for TCP connection to establish sequence tracking
            try:
                data, addr = udp_socket.recvfrom(BUFFER_SIZE)
                
                # Parse the sequence number and timestamp from the packet payload
                received_seq_num, timestamp = data.decode().split(',')
                received_seq_num = int(received_seq_num)

                # Check for missed packets based on the sequence number
                if received_seq_num != last_sequence_number + 1 and last_sequence_number != 0:
                    missed_count = received_seq_num - last_sequence_number - 1
                    missed_packet_log = (
                        f"Missed {missed_count} packet(s) starting from {last_sequence_number + 1} to {received_seq_num - 1} "
                        f"from {addr} at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
                    )
                    log_message(log_widget, missed_packet_log, "missed_packet")
                    with open(log_file, "a") as log:
                        log.write(missed_packet_log + "\n")

                # Log the current received packet
                received_log = f"Received packet {received_seq_num} from {addr} at {timestamp}"
                log_message(log_widget, received_log)
                with open(log_file, "a") as log:
                    log.write(received_log + "\n")

                # Update last sequence number
                last_sequence_number = received_seq_num

            except OSError:
                # Handle socket closure during shutdown
                break

            # Clear tracking if TCP connection drops
            if not reset_tracking.is_set():
                log_message(log_widget, "Clearing sequence number tracking due to TCP disconnection.")
                last_sequence_number = 0


# Start server threads
def start_servers(tcp_interval, log_widget=None, start_button=None, stop_button=None):
    global tcp_thread, udp_thread, server_running
    server_running = True
    if start_button:
        start_button.config(state=tk.DISABLED)
    if stop_button:
        stop_button.config(state=tk.NORMAL)

    tcp_thread = threading.Thread(target=tcp_cip_cm_server, args=(tcp_interval, log_widget), daemon=True)
    udp_thread = threading.Thread(target=udp_io_data_server, args=(log_widget,), daemon=True)

    tcp_thread.start()
    udp_thread.start()

# Stop server threads
def stop_servers(log_widget=None, start_button=None, stop_button=None):
    global server_running
    server_running = False
    reset_tracking.clear()
    log_message(log_widget, "Servers stopped.")
    if start_button:
        start_button.config(state=tk.NORMAL)
    if stop_button:
        stop_button.config(state=tk.DISABLED)

# Function to log messages to GUI or terminal based on mode
def log_message(log_widget, message, tag=None):
    timestamped_message = f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} - {message}"
    if is_headless or log_widget is None:
        # Print to terminal in headless mode
        print(timestamped_message)
    else:
        # Log to GUI in GUI mode
        log_widget.insert(tk.END, timestamped_message + "\n", tag)
        log_widget.see(tk.END)

# GUI setup
def setup_gui():
    root = tk.Tk()
    root.title("CIP Server")

    root.grid_rowconfigure(0, weight=1)
    root.grid_columnconfigure(0, weight=1)

    log_display = ScrolledText(root, state='normal', wrap='word')
    log_display.grid(row=0, column=0, columnspan=2, padx=10, pady=10, sticky="nsew")
    log_display.config(state='normal')

    log_display.tag_config("missed_packet", foreground="red")

    start_button = tk.Button(root, text="Start Server", command=lambda: start_servers(1, log_display, start_button, stop_button))
    start_button.grid(row=1, column=0, padx=10, pady=10, sticky="ew")

    stop_button = tk.Button(root, text="Stop Server", command=lambda: stop_servers(log_display, start_button, stop_button))
    stop_button.grid(row=1, column=1, padx=10, pady=10, sticky="ew")
    stop_button.config(state=tk.DISABLED)

    root.grid_rowconfigure(1, weight=0)
    root.grid_columnconfigure(1, weight=0)

    root.protocol("WM_DELETE_WINDOW", lambda: on_closing(root, log_display, start_button, stop_button))
    root.mainloop()

# Handle GUI close
def on_closing(root, log_widget, start_button, stop_button):
    stop_servers(log_widget, start_button, stop_button)
    root.destroy()

if __name__ == "__main__":
    # Check for --no-gui argument or lack of display environment
    if "--no-gui" in sys.argv or not GUI_AVAILABLE or not os.getenv("DISPLAY"):
        is_headless = True
        print("Starting in headless mode...")
        start_servers(1)
        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            print("\nStopping server...")
            stop_servers()
    else:
        setup_gui()
