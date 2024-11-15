from collections import deque
import math
import threading


class LatencySmoother:
    def __init__(self, max_samples=100, filter_factor=2, min_samples_for_outlier_check=5, logger_app=None, debug=False):
        self.rtt_samples = deque(maxlen=max_samples)  # Circular buffer for RTT samples
        self.filter_factor = filter_factor  # Outlier threshold in standard deviations
        self.min_samples_for_outlier_check = min_samples_for_outlier_check  # Minimum samples before filtering
        self.logger_app = logger_app
        self.class_name = self.__class__.__name__
        self.debug = debug
        self.running_mean = 0
        self.running_std_dev = 0
        self.lock = threading.Lock()  # Lock for thread-safe operations

    def _calculate_running_mean_and_std(self):
        """Update the running mean and standard deviation based on current samples."""
        if not self.rtt_samples:
            self.running_mean, self.running_std_dev = 0, 0
        else:
            self.running_mean = sum(self.rtt_samples) / len(self.rtt_samples)
            variance = sum((x - self.running_mean) ** 2 for x in self.rtt_samples) / len(self.rtt_samples)
            self.running_std_dev = math.sqrt(variance)

    def add_rtt_sample(self, rtt_ns):
        """Add an RTT sample, filtering out outliers."""
        #with self.lock:  # Ensure thread safety
        if rtt_ns > 0:
            if len(self.rtt_samples) < self.min_samples_for_outlier_check or abs(rtt_ns - self.running_mean) <= self.filter_factor * self.running_std_dev:
                self.rtt_samples.append(rtt_ns)
                # Recalculate running mean and std dev after adding a sample
                self._calculate_running_mean_and_std()
                if self.debug:
                    self.logger_app.info(f"{self.class_name}: Added RTT sample: {rtt_ns / 1_000_000}ms (mean: {self.running_mean / 1_000_000}ms, std_dev: {self.running_std_dev / 1_000_000}ms)")
            else:
                if self.debug:
                    self.logger_app.warning(f"{self.class_name}: Discarded outlier RTT sample: {rtt_ns / 1_000_000}ms (mean: {self.running_mean / 1_000_000}ms, std_dev: {self.running_std_dev / 1_000_000}ms)")
        else:
            if self.debug:
                self.logger_app.warning(f"{self.class_name}: Discarded RTT sample <= 0: {rtt_ns / 1_000_000}ms")

    def get_smoothed_latency(self):
        """Return the current smoothed latency (one-way latency)."""
        #with self.lock:  # Ensure thread safety
        return self.running_mean, self.running_mean / 2  # Use precise division to avoid integer bias

    def get_sample_count(self):
        """Return the current number of RTT samples."""
        #with self.lock:  # Ensure thread safety
        return len(self.rtt_samples)
