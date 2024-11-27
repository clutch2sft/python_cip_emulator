from collections import deque
from asyncio import Queue

class QueueManager:
    def __init__(self, queue_configs=None):
        """
        Initialize queues with optional configurations.
        
        Args:
            queue_configs (dict): A dictionary where keys are queue names and values are
                                  configurations like {"type": "Queue"/"deque", "maxsize": int}.
                                  Example:
                                  {
                                      "timestamp_queue": {"type": "Queue", "maxsize": 1000},
                                      "shared_deque": {"type": "deque"}
                                  }
        """
        self.queues = {}
        # Predefined queues with default sizes
        default_configs = {
            "timestamp_queue": {"type": "Queue", "maxsize": 1000},
            "processing_queue": {"type": "Queue", "maxsize": 500},
            "shared_deque": {"type": "deque"}
        }

        # Merge default configs with user-provided configs
        queue_configs = queue_configs or {}
        merged_configs = {**default_configs, **queue_configs}

        for name, config in merged_configs.items():
            self.add_queue(name, config)

    def add_queue(self, name, config):
        """
        Add a new queue dynamically.
        
        Args:
            name (str): The name of the queue.
            config (dict): Configuration for the queue.
                           Example: {"type": "Queue", "maxsize": 500}
        """
        queue_type = config.get("type")
        if queue_type == "Queue":
            maxsize = config.get("maxsize", 0)
            self.queues[name] = Queue(maxsize=maxsize)
            print(f"Queue '{name}' added with maxsize {maxsize}")
        elif queue_type == "deque":
            self.queues[name] = deque()
            print(f"Deque '{name}' added.")
        else:
            raise ValueError(f"Unsupported queue type: {queue_type}")


    def delete_queue(self, name):
        """
        Delete a queue dynamically.
        
        Args:
            name (str): The name of the queue to delete.
        """
        if name in self.queues:
            del self.queues[name]
            print(f"Queue '{name}' deleted.")
        else:
            raise KeyError(f"Queue '{name}' does not exist.")


    def get_queue(self, name):
        """
        Retrieve a queue by its name.
        
        Args:
            name (str): The name of the queue.
            
        Returns:
            Queue/deque: The requested queue, or None if not found.
        """
        return self.queues.get(name)

    def list_queues(self):
        """
        List all available queues in the manager.
        
        Returns:
            list: A list of queue names.
        """
        return list(self.queues.keys())
