import threading

class AtomicInteger:
    def __init__(self, initial=1):
        self.value = initial
        self._lock = threading.Lock()

    def get_and_increment(self, increment_value=1):
        with self._lock:
            current_value = self.value
            self.value += increment_value
            return current_value