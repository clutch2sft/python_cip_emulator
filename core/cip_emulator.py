import asyncio
import time
from core.cip_server import CIPServer
from core.cip_client import CIPClient
from core.time.server.timesyncserver import TimeSyncServer
from core.time.client.timesyncclient import TimeSyncClient


class CIPEmulator:
    def __init__(self, app_config, consumer_config, producers_config, logger_server=None, logger_client=None, logger_app=None, gui_mode=False, quiet=False):
        """
        CIPEmulator initializes and controls the server and client components based on provided configurations.
        """
        self.app_config = app_config
        self.consumer_config = consumer_config
        self.producers_config = producers_config
        self.gui_mode = gui_mode
        self.quiet = quiet
        self.logger_app = logger_app
        self.class_name = self.__class__.__name__

        # Logging setup
        self.server_logger = self._wrap_logger(logger_server) if logger_server else self._null_logger
        self.client_loggers = logger_client or {}

        # Time sync server and client
        self.tsync_server = TimeSyncServer(logger_app=self.logger_app) if not gui_mode else None
        self.tsync_client = None

        # CIPServer
        self.server = CIPServer(self.server_logger, consumer_config) if logger_server else None

        # CIPClients
        self.producers = [
            CIPClient(self._create_client_logger(tag), config, tag=tag, quiet=self.quiet, logger_app=self.logger_app)
            for tag, config in producers_config.items()
        ]

    def _wrap_logger(self, logger):
        """
        Wrap a logger to ensure it can be called uniformly regardless of its original structure.
        If the logger is None, use a no-op logger instead.
        """
        if logger is None:
            return lambda message, level="INFO": None  # No-op logger
        if callable(logger):
            return logger

        def wrapped_logger(message, level="INFO"):
            log_method = getattr(logger, level.lower(), logger.info)
            log_method(message, log_to_console_cancel=self.quiet)

        return wrapped_logger

    def _create_client_logger(self, client_tag):
        """
        Create or retrieve a logger for the client, appending the tag to messages.
        """
        client_logger = self.client_loggers.get(client_tag, self._null_logger)
        return lambda message, level="INFO": getattr(client_logger, level.lower(), 
            client_logger.info)(
            f"[{client_tag}] {message}", log_to_console_cancel=self.quiet
        )

    def _null_logger(self, message, level="INFO"):
        """A placeholder logger."""
        pass

    async def _initialize_time_sync_client(self):
        """
        Initialize and wait for stability in the TimeSyncClient.
        """
        if not self.tsync_client:
            try:
                self.tsync_client = TimeSyncClient(
                    server_ip=self.consumer_config.get("server_ip"),
                    logger_app=self.logger_app,
                    server_port=self.consumer_config.get("time_sync_port", 5555),
                    txrate=0.1,
                    filter_factor=self.consumer_config.get("latency_filter_factor"),
                )
                self.logger_app.info(f"{self.class_name}: Starting TimeSyncClient.")
                if not self.tsync_client.start():
                    self.logger_app.error(f"{self.class_name}: TimeSyncServer unreachable.")
                    return False

                start_time = time.time()
                while not self.tsync_client.is_stable():
                    if time.time() - start_time > 20:  # Timeout after 20 seconds
                        self.logger_app.warning(f"{self.class_name}: TimeSyncClient stability timeout.")
                        return False
                    try:
                        await asyncio.sleep(0.1)
                    except asyncio.CancelledError:
                        self.logger_app.info(f"{self.class_name}: TimeSyncClient initialization cancelled.")
                        raise

                self.logger_app.info(f"{self.class_name}: TimeSyncClient is stable.")
                return True
            except Exception as e:
                self.logger_app.error(f"{self.class_name}: Failed to initialize TimeSyncClient: {e}")
                return False

    async def start_server(self):
        """
        Start the CIPServer and TimeSyncServer asynchronously.
        """
        tasks = []

        if self.server:
            try:
                self.logger_app.info(f"{self.class_name}: Starting CIPServer.")
                tasks.append(self.server.start())
            except Exception as e:
                self.logger_app.error(f"{self.class_name}: Failed to start CIPServer: {e}")

        if self.tsync_server:
            try:
                self.logger_app.info(f"{self.class_name}: Starting TimeSyncServer.")
                tasks.append(self.tsync_server.start())
            except Exception as e:
                self.logger_app.error(f"{self.class_name}: Failed to start TimeSyncServer: {e}")

        if tasks:
            try:
                await asyncio.gather(*tasks)
            except asyncio.CancelledError:
                self.logger_app.warning(f"{self.class_name}: Cancellation received during server startup.")
                await self.stop_server()
                raise KeyboardInterrupt()

    async def stop_server(self):
        """
        Stop the CIPServer and TimeSyncServer.
        """
        try:
            if self.server:
                try:
                    self.logger_app.info(f"{self.class_name}: Stopping CIPServer.")
                    await self.server.stop()
                    self.logger_app.info(f"{self.class_name}: CIPServer stopped.")
                except Exception as e:
                    self.logger_app.error(f"{self.class_name}: Failed to stop CIPServer: {e}")

            if self.tsync_server:
                try:
                    self.logger_app.info(f"{self.class_name}: Stopping TimeSyncServer.")
                    await self.tsync_server.stop()
                    self.logger_app.info(f"{self.class_name}: TimeSyncServer stopped.")
                except asyncio.CancelledError:
                    self.logger_app.warning(f"{self.class_name}: Cancellation during TimeSyncServer shutdown.")
                    raise
                except Exception as e:
                    self.logger_app.error(f"{self.class_name}: Failed to stop TimeSyncServer: {e}")
        finally:
            self.logger_app.info(f"{self.class_name}: All servers stopped.")

    async def start_all_clients(self):
        """
        Start all CIPClients and ensure TimeSyncClient stability.
        """
        if await self._initialize_time_sync_client():
            self.logger_app.info(f"{self.class_name}: Starting all clients.")
            await asyncio.gather(*[client.start() for client in self.producers])

    async def stop_all_clients(self):
        """
        Stop all CIPClients and the TimeSyncClient.
        """
        self.logger_app.info(f"{self.class_name}: Stopping all clients.")
        await asyncio.gather(*[client.stop() for client in self.producers])
        if self.tsync_client:
            self.tsync_client.stop()
            self.tsync_client = None
            self.logger_app.info(f"{self.class_name}: TimeSyncClient stopped.")

    async def run(self):
        """
        Main entry point for running the emulator.
        """
        try:
            await self.start_server()
            await self.start_all_clients()
        except asyncio.CancelledError:
            self.logger_app.info(f"{self.class_name}: Emulator cancelled.")
            raise KeyboardInterrupt()
        finally:
            self.logger_app.info(f"{self.class_name}: Stopping all clients.")
            await self.stop_all_clients()
            self.logger_app.info(f"{self.class_name}: Stopping server.")
            await self.stop_server()
