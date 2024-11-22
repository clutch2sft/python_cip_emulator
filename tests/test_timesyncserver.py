import pytest
import pytest_asyncio
from unittest.mock import MagicMock
from core.time.server.timesyncserver import TimeSyncServer, TimeSyncService
from core.time.server import timesync_pb2


@pytest_asyncio.fixture
async def timesync_server(logger):
    """Fixture to start and stop a TimeSyncServer."""
    print("Initializing TimeSyncServer fixture...")
    server = TimeSyncServer(ip_address="127.0.0.1", port=50051, logger_app=logger, debug=True)
    print("Starting TimeSyncServer...")
    await server.start()  # Start the server
    print("TimeSyncServer started.")
    try:
        yield server          # Provide the instance to tests
    finally:
        print("Stopping TimeSyncServer...")
        await server.stop()   # Stop the server after tests
        print("TimeSyncServer stopped.")
        server.close()        # Clean up
        print("TimeSyncServer closed.")

@pytest.fixture
def logger():
    """Fixture to create a mock logger."""
    mock_logger = MagicMock()
    mock_logger.info = MagicMock()
    mock_logger.warning = MagicMock()
    return mock_logger

@pytest.mark.asyncio
async def test_server_start_stop(timesync_server):
    """Test that the TimeSyncServer starts and stops cleanly."""
    assert not timesync_server.stopped, "Server should be running after start."
    await timesync_server.stop()
    assert timesync_server.stopped, "Server should be stopped after stop."


@pytest.mark.asyncio
async def test_keyboard_interrupt_handling(timesync_server):
    """Test that the server handles simulated shutdown cleanly."""
    await timesync_server.stop()
    assert timesync_server.stopped, "Server should stop cleanly when shutdown is triggered."


@pytest.mark.asyncio
async def test_fixture_directly(timesync_server):
    """Directly test the timesync_server fixture."""
    assert isinstance(timesync_server, TimeSyncServer), "Fixture should return a TimeSyncServer instance."
    assert not timesync_server.stopped, "Server should be running after fixture setup."
    await timesync_server.stop()
    assert timesync_server.stopped, "Server should be stopped after stopping."
