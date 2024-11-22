import asyncio
import signal
import logging
from core.time.server.timesyncserver import TimeSyncServer

# Configure a basic logger
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("TimeSyncTest")

async def main():
    """
    Main entry point for testing the TimeSyncServer.
    """
    logger.info("Test script: Initializing TimeSyncServer...")

    # Create the TimeSyncServer instance
    server = TimeSyncServer(logger_app=logger, debug=True)

    # Register signal handlers for graceful shutdown
    stop_event = asyncio.Event()

    def signal_handler(sig, frame):
        logger.info(f"Signal {sig} received. Initiating shutdown...")
        stop_event.set()

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    # Start the server
    server_task = asyncio.create_task(server.start())

    try:
        logger.info("Test script: Server is running. Press Ctrl+C to stop...")
        # Wait for the stop event to be set (via Ctrl+C)
        await stop_event.wait()
    finally:
        logger.info("Test script: Stopping the server...")
        await server.stop()
        await server_task
        server.close()

    logger.info("Test script: TimeSyncServer shutdown complete.")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Keyboard interrupt received. Exiting...")
