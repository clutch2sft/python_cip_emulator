import json
import os

def load_config(config_path="config.json"):
    """Load configuration from a JSON file."""
    if not os.path.exists(config_path):
        raise FileNotFoundError(f"Configuration file not found: {config_path}")
    
    with open(config_path, "r") as config_file:
        config = json.load(config_file)
    
    # Verify that all expected keys are present and assign defaults if needed
    default_config = {
        "tcp_port": 1502,
        "udp_port": 1200,
        "packet_interval_ms": 100,
        "buffer_size": 1024,
        "log_directory": "./logs",
        "enable_packet_skip": False
    }
    # Update defaults with values from config file
    default_config.update(config)
    
    # Ensure log directory exists
    os.makedirs(default_config["log_directory"], exist_ok=True)

    return default_config
