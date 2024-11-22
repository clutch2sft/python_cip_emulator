import asyncio
from core.time.server.timesyncserver import TimeSyncServer
from unittest.mock import MagicMock

async def main():
    logger = MagicMock()
    logger.info = MagicMock()
    logger.warning = MagicMock()

    server = TimeSyncServer(ip_address="127.0.0.1", port=50051, logger_app=logger, debug=True)
    await server.start()
    print("Server started")
    await asyncio.sleep(1)  # Simulate some uptime
    await server.stop()
    print("Server stopped")
    server.close()

if __name__ == "__main__":
    asyncio.run(main())
