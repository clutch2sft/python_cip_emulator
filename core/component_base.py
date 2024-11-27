class ComponentBase:
    def __init__(self, name, logger):
        self.name = name
        self.logger = logger or self._null_logger

    def _null_logger(self):
        """Return a no-op logger with all necessary methods."""
        class NoOpLogger:
            def info(self, *args, **kwargs):
                pass

            def warning(self, *args, **kwargs):
                pass

            def error(self, *args, **kwargs):
                pass

            def debug(self, *args, **kwargs):
                pass

        return NoOpLogger()


    async def start(self):
        """Start the component."""
        self.logger.info(f"Starting {self.name}")

    async def stop(self):
        """Stop the component."""
        self.logger.info(f"Stopping {self.name}")
