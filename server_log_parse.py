import re
from datetime import datetime


# Define the path to the log file (update if necessary)
#log_file = r"C:\Users\grecampb\OneDrive - Cisco\Documents\FZ_IW9165_Troubles\CIP Emulator Logs\PC Side\log_20241107_125008_server_packet.txt"  # Replace with the actual log filename

log_file = r"C:\Users\grecampb\OneDrive - Cisco\Documents\FZ_IW9165_Troubles\CIP Emulator Logs\IW9165_Client Side\log_20241107_130735_server_packet.txt"  # Replace with the actual log filename
packet_interval_ms = 100  # Expected packet interval in milliseconds

# Function to parse timestamps and sequence numbers from log lines
def parse_log_line(line):
    match = re.search(r"Received packet (\d+) .* at (\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d+)", line)
    if match:
        seq_num = int(match.group(1))
        timestamp = datetime.strptime(match.group(2), "%Y-%m-%d %H:%M:%S.%f")
        return seq_num, timestamp
    return None, None

# Function to parse missed packets from log
def parse_missed_packets(log_file):
    line_before_data = None  # Holds the last processed "Received packet" data before a missed packet
    missed_triggered = False  # Indicates if a "missed packet" line was encountered
    
    try:
        with open(log_file, "r") as file:
            for line in file:
                # Process "Received packet" lines
                if "Received packet" in line:
                    seq_num, timestamp = parse_log_line(line)
                    
                    if missed_triggered:
                        # We found the line after the missed packet
                        # Calculate the time delta from line_before to the current line
                        prev_seq_num, prev_timestamp = line_before_data
                        time_delta = (timestamp - prev_timestamp).total_seconds() * 1000  # Convert to milliseconds
                        estimated_missed_packets = int(time_delta / packet_interval_ms) - 1
                        
                        print(f"At {prev_timestamp} - Estimated {estimated_missed_packets} missed packet(s) between sequence {prev_seq_num} and {seq_num} "
                              f"(Time delta: {time_delta:.2f} ms)")
                        
                        # Reset missed trigger and line_before_data
                        missed_triggered = False
                        line_before_data = (seq_num, timestamp)

                    else:
                        # Update line_before_data as no missed packet was encountered
                        line_before_data = (seq_num, timestamp)
                
                # Detect "Missed packet" lines and set trigger
                elif "Missed packet" in line:
                    missed_triggered = True  # Set the trigger to indicate a gap was found

    except FileNotFoundError:
        print(f"Log file not found: {log_file}")
    except Exception as e:
        print(f"An error occurred while reading the log file: {e}")

# Run the parser
parse_missed_packets(log_file)
