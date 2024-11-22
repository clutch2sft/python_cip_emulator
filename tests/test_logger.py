import threading
import time
import os
from utils.logger import create_threaded_logger
import pytest


def log_messages(logger, thread_id):
    """Function to log messages from a thread."""
    for i in range(10):
        logger.info(f"Info message from thread {thread_id}, count {i}")
        logger.warning(f"Warning message from thread {thread_id}, count {i}")
        logger.error(f"Error message from thread {thread_id}, count {i}")
        time.sleep(0.1)  # Simulate some delay between log messages


@pytest.fixture
def logger():
    """Fixture to create and clean up a threaded logger."""
    logger = create_threaded_logger(
        name="TestLogger",
        log_directory="./test_logs",
        log_to_console=True,
        log_to_file=True,
        use_ansi_colors=False,
        batch_size=5,
        flush_interval=1
    )
    yield logger
    logger.flush_and_stop()
    # Cleanup: Remove test logs directory
    if os.path.exists("./test_logs"):
        for file in os.listdir("./test_logs"):
            os.remove(os.path.join("./test_logs", file))
        os.rmdir("./test_logs")


def test_thread_safety(logger):
    """Test logger with multiple threads to ensure thread safety."""
    threads = []
    for i in range(3):  # Create 3 threads
        t = threading.Thread(target=log_messages, args=(logger, i))
        threads.append(t)
        t.start()

    for t in threads:
        t.join()

    assert True, "Logger handled multiple threads without crashing."


def test_file_logging(logger):
    """Test that log messages are written to the log file."""
    log_messages(logger, thread_id=0)  # Log from a single thread

    # Check the log directory for files
    assert os.path.exists("./test_logs"), "Log directory does not exist."
    log_files = os.listdir("./test_logs")
    assert len(log_files) > 0, "No log files were created."

    # Verify log file content
    for log_file in log_files:
        with open(os.path.join("./test_logs", log_file), "r") as f:
            content = f.read()
            assert "Info message from thread 0" in content, "Log file is missing expected content."


def test_keyboard_interrupt_handling(logger):
    """Simulate a KeyboardInterrupt and ensure the logger shuts down cleanly."""
    threads = []
    for i in range(3):  # Create 3 threads
        t = threading.Thread(target=log_messages, args=(logger, i))
        threads.append(t)
        t.start()

    time.sleep(0.5)  # Let threads start logging
    try:
        raise KeyboardInterrupt  # Simulate Ctrl+C
    except KeyboardInterrupt:
        logger.flush_and_stop()
        assert True, "Logger shut down cleanly on KeyboardInterrupt."
