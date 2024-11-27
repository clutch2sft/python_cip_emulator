from collections import deque
import math


class LatencySmoother:
    def __init__(self, rtt_deque, filter_factor=2, min_samples_for_outlier_check=5, min_queue_depth=5, logger_app=None, debug=False):
        """
        Initialize the LatencySmoother.

        Args:
            rtt_deque (deque): A shared deque for RTT samples.
            filter_factor (int): Outlier threshold in standard deviations.
            min_samples_for_outlier_check (int): Minimum samples before filtering outliers.
            min_queue_depth (int): Minimum number of samples in the queue before outlier detection is valid.
            logger_app: Logger instance for logging.
            debug (bool): Enable debug logging.
        """
        self.rtt_samples = rtt_deque  # Shared deque for RTT samples
        self.filter_factor = filter_factor  # Outlier threshold
        self.min_samples_for_outlier_check = min_samples_for_outlier_check  # Minimum samples before filtering
        self.min_queue_depth = min_queue_depth  # Minimum depth for meaningful outlier detection
        self.logger_app = logger_app
        self.class_name = self.__class__.__name__
        self.debug = debug
        self.running_mean = 0
        self.running_std_dev = 0

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
        if rtt_ns > 0:
            if (
                len(self.rtt_samples) < self.min_samples_for_outlier_check
                or abs(rtt_ns - self.running_mean) <= self.filter_factor * self.running_std_dev
            ):
                self.rtt_samples.append(rtt_ns)
                self._calculate_running_mean_and_std()
                if self.debug:
                    self.logger_app.info(
                        f"{self.class_name}: Added RTT sample: {rtt_ns / 1_000_000}ms "
                        f"(mean: {self.running_mean / 1_000_000}ms, std_dev: {self.running_std_dev / 1_000_000}ms)"
                    )
            else:
                if self.debug:
                    self.logger_app.warning(
                        f"{self.class_name}: Discarded outlier RTT sample: {rtt_ns / 1_000_000}ms "
                        f"(mean: {self.running_mean / 1_000_000}ms, std_dev: {self.running_std_dev / 1_000_000}ms)"
                    )
        else:
            if self.debug:
                self.logger_app.warning(
                    f"{self.class_name}: Discarded RTT sample <= 0: {rtt_ns / 1_000_000}ms"
                )

    def is_outlier(self, rtt_ns):
        """
        Check if an RTT sample is an outlier.

        Args:
            rtt_ns (int): RTT sample in nanoseconds.

        Returns:
            (bool, float, float): True if outlier, mean, std_dev (or None, None if not enough samples).
        """
        if rtt_ns <= 0:
            return False, None, None

        if len(self.rtt_samples) < self.min_queue_depth:
            # Not enough data to make a meaningful determination
            if self.debug:
                self.logger_app.info(f"{self.class_name}: Insufficient samples for outlier detection.")
            return False, None, None

        mean = self.running_mean
        std_dev = self.running_std_dev
        threshold = mean + self.filter_factor * std_dev

        is_outlier = abs(rtt_ns - mean) > threshold

        if self.debug:
            self.logger_app.info(
                f"{self.class_name}: Checked RTT sample: {rtt_ns / 1_000_000}ms "
                f"(mean: {mean / 1_000_000}ms, std_dev: {std_dev / 1_000_000}ms, threshold: {threshold / 1_000_000}ms) "
                f"-> Outlier: {is_outlier}"
            )

        return is_outlier, mean, std_dev

    def get_is_outlier(self, rtt_ns):
        """
        Add an RTT sample and determine if it is an outlier. Discard the sample if it is an outlier.

        Args:
            rtt_ns (int): RTT sample in nanoseconds.

        Returns:
            bool: True if the RTT is an outlier, False otherwise.
        """
        is_outlier, mean, std_dev = self.is_outlier(rtt_ns)

        if not is_outlier:
            self.rtt_samples.append(rtt_ns)
            self._calculate_running_mean_and_std()
            if self.debug:
                self.logger_app.info(
                    f"{self.class_name}: Added RTT sample: {rtt_ns / 1_000_000}ms "
                    f"(mean: {self.running_mean / 1_000_000}ms, std_dev: {self.running_std_dev / 1_000_000}ms)"
                )
        else:
            if self.debug:
                self.logger_app.warning(
                    f"{self.class_name}: Discarded outlier RTT sample: {rtt_ns / 1_000_000}ms "
                    f"(mean: {mean / 1_000_000}ms, std_dev: {std_dev / 1_000_000}ms)"
                )

        return is_outlier, mean, std_dev

    def get_smoothed_latency(self):
        """Return the current smoothed latency (one-way latency)."""
        return self.running_mean, self.running_mean / 2  # One-way latency approximation

    def get_sample_count(self):
        """Return the current number of RTT samples."""
        return len(self.rtt_samples)
