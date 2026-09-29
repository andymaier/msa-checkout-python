"""Kafka-Anbindung: Operation, Producer, ShopListener.

TODO (Uebung): den Listener implementieren.
"""
import json
import threading
from dataclasses import dataclass
from typing import Any

from kafka import KafkaConsumer, KafkaProducer

from .config import KAFKA_BOOTSTRAP_SERVERS, SHOP_TOPIC, KAFKA_GROUP_ID
from .store import prices


@dataclass
class Operation:
    bo: str
    action: str
    object: Any = None

    @staticmethod
    def from_bytes(raw: bytes) -> "Operation":
        d = json.loads(raw.decode("utf-8"))
        return Operation(d.get("bo"), d.get("action"), d.get("object"))

    def to_bytes(self) -> bytes:
        return json.dumps(
            {"bo": self.bo, "action": self.action, "object": self.object}
        ).encode("utf-8")


class ShopProducer:
    """Sendet Operation-Events auf 'shop'. Verbindung wird verzoegert
    aufgebaut, damit die App auch ohne laufendes Kafka startet."""

    def __init__(self):
        self._producer = None

    def _get(self):
        if self._producer is None:
            self._producer = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                value_serializer=lambda op: op.to_bytes(),
            )
        return self._producer

    def send(self, op: "Operation"):
        self._get().send(SHOP_TOPIC, op).get(timeout=10)


class ShopListener:
    def __init__(self):
        self.prices = prices

    def handle(self, op: Operation):
        # TODO: Operation verarbeiten.
        #   - nur bo == "article" ist relevant
        #   - object enthaelt {uuid, price}
        #   - action "upsert": Preis in `self.prices` setzen (nur wenn price != None;
        #     vorhandenen Preis beibehalten, falls schon gesetzt - wie im Original)
        #   - action "delete": Preis aus `self.prices` entfernen
        raise NotImplementedError("ShopListener.handle noch nicht implementiert")

    def start(self):
        threading.Thread(target=self._consume, daemon=True).start()

    def _consume(self):
        consumer = KafkaConsumer(
            SHOP_TOPIC,
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            group_id=KAFKA_GROUP_ID,
            auto_offset_reset="earliest",
            value_deserializer=Operation.from_bytes,
        )
        for msg in consumer:
            try:
                self.handle(msg.value)
            except Exception as e:
                print(f"[checkout] Fehler beim Verarbeiten: {e}")
