"""In-Memory-Preisspeicher (Pendant zur ConcurrentHashMap<uuid, BigDecimal>).
Vollstaendig vorgegeben - nicht Teil der Uebung."""
import threading


class PriceStore:
    def __init__(self):
        self._prices = {}
        self._lock = threading.Lock()

    def get(self, uuid):
        with self._lock:
            return self._prices.get(uuid)

    def set(self, uuid, price):
        with self._lock:
            self._prices[uuid] = price

    def delete(self, uuid):
        with self._lock:
            self._prices.pop(uuid, None)

    def count(self):
        with self._lock:
            return len(self._prices)


# gemeinsame Instanz fuer API und Listener
prices = PriceStore()
