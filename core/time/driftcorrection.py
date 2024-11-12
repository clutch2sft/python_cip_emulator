from collections import deque

class DriftCorrector:
    def __init__(self, max_samples=250, network_latency_ns=0):
        self.discrepancies = deque(maxlen=max_samples)  # Circular buffer for 250 samples
        self.network_latency_ns = network_latency_ns  # Estimated one-way network latency

    def add_discrepancy(self, discrepancy_ns, client_ahead):
        # Store signed discrepancy: positive if client ahead, negative if client behind
        signed_discrepancy = discrepancy_ns if client_ahead else -discrepancy_ns
        self.discrepancies.append(signed_discrepancy)

    def calculate_mean_drift(self):
        # Calculate the average signed discrepancy (mean drift)
        if not self.discrepancies:
            return 0  # Return 0 if no discrepancies have been recorded

        mean_discrepancy = sum(self.discrepancies) / len(self.discrepancies)
        
        # Adjust mean discrepancy by network latency for a more accurate drift
        corrected_drift = mean_discrepancy - self.network_latency_ns if mean_discrepancy > 0 else mean_discrepancy + self.network_latency_ns
        return corrected_drift

