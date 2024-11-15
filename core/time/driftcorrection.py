from collections import deque
import math
import threading


class DriftCorrectorBorg:
    _shared_state = {}  # Shared state dictionary

    def __init__(self, max_samples=250, network_latency_ns=0, filter_factor=2, min_samples_for_outlier_check=5, logger_app=None, debug=False):
        self.__dict__ = self._shared_state  # Share the state among all instances
        if not hasattr(self, 'initialized'):  # Initialize once
            self.discrepancies = deque(maxlen=max_samples)
            self.network_latency_ns = network_latency_ns
            self.filter_factor = filter_factor
            self.min_samples_for_outlier_check = min_samples_for_outlier_check  # Minimum samples before filtering
            self.logger_app = logger_app
            self.meandrift = 0
            self.stddeviation = 0
            self.class_name = self.__class__.__name__
            self.debug = debug
            self.lock = threading.Lock()  # Add a lock for thread-safe operations
            self.initialized = True

    def _calculate_mean_and_std(self):
        """Calculate mean and standard deviation of current discrepancies."""
        with self.lock:  # Ensure thread safety
            if not self.discrepancies:
                self.logger_app.warning(f"{self.class_name}: There are no discrepancies to work with")
                return 0, 0  # Return 0s if there are no discrepancies

            mean = sum(self.discrepancies) / len(self.discrepancies)
            variance = sum((x - mean) ** 2 for x in self.discrepancies) / len(self.discrepancies)
            std_dev = math.sqrt(variance)
            self.meandrift = mean
            self.stddeviation = std_dev
            return mean, std_dev

    def add_discrepancy(self, discrepancy_ns, client_ahead):
        """Add a discrepancy, filtering out outliers."""
        signed_discrepancy = discrepancy_ns if client_ahead else -discrepancy_ns
        with self.lock:  # Ensure thread safety
            # Calculate current mean and standard deviation
            mean, std_dev = self._calculate_mean_and_std()

            # Only apply outlier filtering if we have enough samples
            if len(self.discrepancies) < self.min_samples_for_outlier_check or abs(signed_discrepancy - mean) <= self.filter_factor * std_dev:
                self.discrepancies.append(signed_discrepancy)
                if self.debug:
                    self.logger_app.info(f"{self.class_name}: Added discrepancy: {signed_discrepancy / 1_000_000}ms (mean: {mean / 1_000_000}ms, std_dev: {std_dev / 1_000_000}ms)")
            else:
                self.logger_app.warning(f"{self.class_name}: Discarded outlier discrepancy: {signed_discrepancy / 1_000_000}ms (mean: {mean / 1_000_000}ms, std_dev: {std_dev / 1_000_000}ms)")

    def calculate_mean_drift(self):
        """Calculate mean drift and standard deviation for reporting."""
        with self.lock:  # Ensure thread safety
            mean, std_dev = self._calculate_mean_and_std()
            # Adjust mean discrepancy by network latency for more accurate drift
            corrected_drift = mean - self.network_latency_ns if mean > 0 else mean + self.network_latency_ns
            return corrected_drift, std_dev

    def get_drift(self):
        """Return the current mean drift."""
        with self.lock:  # Ensure thread safety
            return self.meandrift

    def get_sample_count(self):
        """Return the current number of discrepancies."""
        with self.lock:  # Ensure thread safety
            return len(self.discrepancies)
