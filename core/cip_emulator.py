import asyncio
import inspect
from core.cip_server import CIPServer
from utils.logger import create_threaded_logger


class CIPEmulator:
    def __init__(self, app_config, consumer_config, producers_config, gui_mode=False, quiet=False, threadmanager=None):
        """
        CIPEmulator initializes and controls components based on the dependency graph.
        """
        self.app_config = app_config
        self.consumer_config = consumer_config
        self.producers_config = producers_config
        self.gui_mode = gui_mode
        self.quiet = quiet
        self.threadmanager = threadmanager

        # Components managed by the dependency graph
        self.components = {}

        # Initialize logger
        self.logger_app = self._wrap_logger(
            create_threaded_logger(threadmanager, name="CIPEmulator_Logger", log_to_console=True, debug=False)
        )
        self.register_component("logger_app", self.logger_app)
        # Initialize Server Logger
        self.logger_server = self._wrap_logger(
            create_threaded_logger(threadmanager, name="CIPServer_Logger", log_to_console=True, debug=False)
        )
        self.register_component("logger_server", self.logger_server)
        # Initialize CIPServer
        self.cip_server = CIPServer(logger=self.logger_server, consumer_config=consumer_config, thread_manager=self.threadmanager)
        self.register_component("cip_server", self.cip_server, dependencies=["logger_app"])

        # Placeholder for CIPClients
        # self.cip_clients = [
        #     self._initialize_client(tag, config) for tag, config in producers_config.items()
        # ]

        # Resolve dependencies during initialization
        self.resolve_dependencies()

    def _wrap_logger(self, logger):
        """
        Wrap a logger to ensure it can be called uniformly regardless of its original structure.
        """
        if logger is None:
            return self._null_logger  # Fallback to no-op logger

        if hasattr(logger, "info"):  # Already a logger with required methods
            return logger

        # Wrap a function-like logger to provide `info`, `error`, etc.
        class FunctionLoggerWrapper:
            def __init__(self, log_func):
                self.log_func = log_func

            def info(self, message):
                self.log_func(message, level="INFO")

            def warning(self, message):
                self.log_func(message, level="WARNING")

            def error(self, message):
                self.log_func(message, level="ERROR")

            def debug(self, message):
                self.log_func(message, level="DEBUG")

        return FunctionLoggerWrapper(logger)





    def _initialize_client(self, tag, config):
        """
        Initialize a CIPClient with its logger.
        """
        logger = self.logger_clients[tag]
        client = CIPServer(logger=logger, consumer_config=config)  # Replace with actual CIPClient
        self.register_component(f"cip_client_{tag}", client, dependencies=[f"logger_client_{tag}"])
        return client

    def register_component(self, name, component, dependencies=None):
        """
        Register a component with optional dependencies in the dependency graph.
        """
        self.components[name] = {
            "instance": component,
            "dependencies": dependencies or [],
        }

    def resolve_dependencies(self):
        """
        Resolve component dependencies using a topological sort.
        """
        resolved_order = []
        unresolved = set(self.components.keys())
        while unresolved:
            for name in list(unresolved):
                dependencies = self.components[name]["dependencies"]
                if all(dep in resolved_order for dep in dependencies):
                    resolved_order.append(name)
                    unresolved.remove(name)

        self.start_order = resolved_order
        self.stop_order = resolved_order[::-1]  # Reverse for shutdown order

    async def start(self):
        """
        Start all components in the correct dependency order.
        """
        try:
            self.logger_app.info("Starting CIPEmulator...")

            for name in self.start_order:
                component = self.components.get(name, {}).get("instance")
                if component is None:
                    raise ValueError(f"Component '{name}' is not defined or missing an instance.")

                self.logger_app.info(f"Starting component: {name}")
                
                # Await the start method (either async_start or start)
                if hasattr(component, "async_start"):
                    await component.async_start()
                else:
                    await component.start()

            self.logger_app.info("CIPEmulator started successfully.")

        except Exception as e:
            self.logger_app.error(f"Error while starting CIPEmulator: {e}")
            raise


    async def stop(self):
        """
        Stop all components in reverse dependency order.
        """
        self.logger_app.info("Stopping CIPEmulator...")

        for name in self.stop_order:
            try:
                component = self.components.get(name, {}).get("instance")
                if component is None:
                    self.logger_app.warning(f"Component '{name}' is missing or invalid during shutdown.")
                    continue

                self.logger_app.info(f"Stopping component: {name}")

                # Ensure the component has a stop method
                if hasattr(component, "stop"):
                    if inspect.iscoroutinefunction(component.stop):
                        await component.stop()
                    else:
                        component.stop()
                else:
                    self.logger_app.warning(f"Component '{name}' does not have a stop method.")

            except Exception as e:
                self.logger_app.error(f"Error stopping component '{name}': {e}")

        self.logger_app.info("CIPEmulator stopped successfully.")

    async def run(self):
        """
        Main entry point for running the emulator.
        """
        try:
            await self.start()
            self.logger_app.info("CIPEmulator is running. Press Ctrl+C to exit.")
            while True:
                await asyncio.sleep(1)  # Keep the emulator running
        except asyncio.CancelledError:
            self.logger_app.warning("CIPEmulator run cancelled.")
        except KeyboardInterrupt:
            self.logger_app.info("CIPEmulator interrupted by user.")
        finally:
            await self.stop()
