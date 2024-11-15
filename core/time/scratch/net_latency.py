# net_latency.py
import statistics
import threading

class NetLatency:
    _instance = None
    _lock = threading.Lock()  # Thread-safe singleton lock

    def __new__(cls, *args, **kwargs):
        if not cls._instance:
            with cls._lock:
                if not cls._instance:
                    cls._instance = super(NetLatency, cls).__new__(cls)
        return cls._instance

    def __init__(self):
        if not hasattr(self, 'rtts'):
            self.rtts = []      # Store non-outlier RTT values
            self.outliers = []  # Store outliers separately

    def add_rtt(self, rtt):
        """Add an RTT value to the list, checking for outliers if enough data is present."""
        if rtt is None:
            return  # Ignore None values

        # Check for outliers only if there are more than 20 RTTs
        if len(self.rtts) >= 20:
            # Calculate current mean and standard deviation of non-outliers
            mean_rtt = statistics.mean(self.rtts)
            std_dev_rtt = statistics.stdev(self.rtts) if len(self.rtts) > 1 else 0.0

            # Check if the new RTT is an outlier
            if abs(rtt - mean_rtt) > 2 * std_dev_rtt:
                self.outliers.append(rtt)
                print(f"Identified outlier: {rtt:.2f} ms")
                return  # Skip adding to `rtts` if it's an outlier

        # Add RTT to non-outlier list if it's not an outlier
        self.rtts.append(rtt)

    def get_statistics(self):
        """Calculate mean and standard deviation, excluding outliers."""
        if not self.rtts:
            return {"mean": None, "std_dev": None, "outliers": self.outliers}

        mean_rtt = statistics.mean(self.rtts)
        std_dev_rtt = statistics.stdev(self.rtts) if len(self.rtts) > 1 else 0.0

        return {
            "mean": mean_rtt,
            "std_dev": std_dev_rtt,
            "outliers": self.outliers
        }

    def clear(self):
        """Clear all stored RTTs and outliers."""
        self.rtts.clear()
        self.outliers.clear()
