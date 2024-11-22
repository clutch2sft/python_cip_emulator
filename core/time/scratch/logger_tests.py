import threading
import time
from utils.logger import create_threaded_logger

def log_messages(logger, thread_id):
    """Function to log messages from a thread."""
    for i in range(10):
        logger.info(f"Info message from thread {thread_id}, count {i}")
        logger.warning(f"Warning message from thread {thread_id}, count {i}")
        logger.error(f"Error message from thread {thread_id}, count {i}")
        time.sleep(0.1)  # Simulate some delay between log messages

def main():
    # Create the threaded logger
    logger = create_threaded_logger(
        name="TestLogger",
        log_directory="./test_logs",
        log_to_console=True,
        log_to_file=True,
        use_ansi_colors=True,
        batch_size=5,
        flush_interval=1
    )

    print("Logger initialized. Starting logging test...")

    # Start logging in multiple threads to test thread safety
    threads = []
    for i in range(3):  # Create 3 threads
        t = threading.Thread(target=log_messages, args=(logger, i))
        threads.append(t)
        t.start()

    # Wait for all threads to finish
    for t in threads:
        t.join()

    print("All threads completed. Initiating logger shutdown...")

    # Flush and stop the logger gracefully
    logger.flush_and_stop()

    print("Logger shutdown complete. Check logs in the 'test_logs' directory.")

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("Interrupted! Initiating shutdown...")
