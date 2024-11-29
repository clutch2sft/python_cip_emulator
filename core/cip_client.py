import asyncio
from cip_tcp_server import CIPTcpServer
from cip_udp_client import CIPUdpClient
from cip_udp_server import CIPUdpServer


class CIPClient:
    def __init__(self, logger, consumer_config, producer_configs, thread_manager, timestamp_queue, drift_corrector):
        """
        Initialize the CIPClient.

        Args:
            logger: Logger instance for logging.
            consumer_config: Configuration for the TCP server.
            producer_configs: Dictionary of configurations for multiple UDP flows.
            thread_manager: Thread manager for handling tasks.
            timestamp_queue: Queue for handling timestamp requests.
            drift_corrector: DriftCorrector instance for UDP timestamp adjustments.
        """
        self.logger = logger
        self.consumer_config = consumer_config
        self.producer_configs = producer_configs
        self.thread_manager = thread_manager
        self.timestamp_queue = timestamp_queue
        self.drift_corrector = drift_corrector

        # Shared resources
        self.connections = {}
        self.tcp_connected_event = asyncio.Event()  # Shared TCP connection event

        # Initialize TCP server
        self.tcp_server = CIPTcpServer(
            logger=self.logger,
            consumer_config=self.consumer_config,
            thread_manager=self.thread_manager,
            timestamp_queue=self.timestamp_queue,
            connections=self.connections,
            tcp_connected_event=self.tcp_connected_event
        )

        # Initialize UDP clients for each producer config
        self.udp_clients = []
        for name, config in self.producer_configs.items():
            udp_client = CIPUdpClient(
                server_ip=config["ip"],
                udp_dstport=config["udp_dstport"],
                udp_srcport=config["udp_srcport"],
                logger=self.logger,
                drift_corrector=self.drift_corrector,
                tcp_connected_event=self.tcp_connected_event,
                qos=config["qos"],
                packet_interval_ms=config["packet_interval_ms"],
                enable_packet_skip=config["enable_packet_skip"],
                tag=name,
                debug=config.get("debug", False)
            )
            self.udp_clients.append(udp_client)

        # Initialize single UDP server
        self.udp_server = CIPUdpServer(
            logger=self.logger,
            consumer_config=self.consumer_config,
            timestamp_queue=self.timestamp_queue
        )

    async def start(self):
        """Start the CIP client."""
        self.logger("CIPClient: Starting CIP client.", level="INFO")

        # Start TCP server, UDP clients, and UDP server concurrently
        await asyncio.gather(
            self._start_tcp_server(),
            self._start_udp_clients(),
            self.udp_server.start()
        )

    async def stop(self):
        """Stop the CIP client."""
        self.logger("CIPClient: Stopping CIP client.", level="INFO")
        await self.tcp_server.stop()
        for udp_client in self.udp_clients:
            udp_client.stop()
        await self.udp_server.stop()

    async def _start_tcp_server(self):
        """Start the TCP server."""
        await self.tcp_server.start()

    async def _start_udp_clients(self):
        """Start all UDP clients."""
        for udp_client in self.udp_clients:
            udp_client.start()
            await udp_client.send_udp_packets()

