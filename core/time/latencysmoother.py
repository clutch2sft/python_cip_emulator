from collections import deque
import math

class LatencySmoother:
    def __init__(self, max_samples=100, filter_factor=2, min_samples_for_outlier_check=5, logger_app=None, debug=False):
        """
        Initialize the LatencySmoother.

        Args:
            max_samples (int): The maximum number of samples to keep.
            filter_factor (float): Number of standard deviations for outlier rejection.
            min_samples_for_outlier_check (int): Minimum samples required before outlier rejection.
        """
        self.rtt_samples = deque(maxlen=max_samples)  # Circular buffer for RTT samples
        self.filter_factor = filter_factor  # Outlier threshold in standard deviations
        self.min_samples_for_outlier_check = min_samples_for_outlier_check  # Minimum samples before filtering
        self.logger_app = logger_app
        self.class_name = self.__class__.__name__
        self.debug = False

    def _calculate_mean_and_std(self):
        """Calculate the mean and standard deviation of the RTT samples."""
        if not self.rtt_samples:
            return 0, 0

        mean = sum(self.rtt_samples) / len(self.rtt_samples)
        variance = sum((x - mean) ** 2 for x in self.rtt_samples) / len(self.rtt_samples)
        std_dev = math.sqrt(variance)
        return mean, std_dev

    def add_rtt_sample(self, rtt_ns):
        """
        Add an RTT sample, filtering out outliers.

        Args:
            rtt_ns (int): Round-trip time in nanoseconds.
        """
        mean, std_dev = self._calculate_mean_and_std()

        # Only add the sample if RTT is greater than 0, within acceptable range, or if not enough samples for outlier rejection
        if rtt_ns > 0:
            if len(self.rtt_samples) < self.min_samples_for_outlier_check or abs(rtt_ns - mean) <= self.filter_factor * std_dev:
                self.rtt_samples.append(rtt_ns)
                if self.debug:
                    self.logger_app.info(f"{self.class_name}: Added RTT sample: {rtt_ns} ns (mean: {mean}, std_dev: {std_dev})")
            else:
                if self.debug:
                    self.logger_app.warning(f"{self.class_name}: Discarded outlier RTT sample: {rtt_ns} ns (mean: {mean}, std_dev: {std_dev})")
        else:
            if self.debug:
                    self.logger_app.warning(f"{self.class_name}: Discarded RTT sample <= 0: {rtt_ns} ns")


    def get_smoothed_latency(self):
        """Return the current average latency."""
        mean, _ = self._calculate_mean_and_std()
        return mean // 2  # Return one-way latency estimate
