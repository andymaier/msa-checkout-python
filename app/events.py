"""Kafka-Anbindung: Operation, Producer, ShopListener - Loesung."""
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
        if op.bo != "article":
            return
        obj = op.object or {}
        uuid = obj.get("uuid")
        price = obj.get("price")
        if op.action == "upsert":
            if price is None:
                return
            old = self.prices.get(uuid)
            self.prices.set(uuid, old if old is not None else price)
        elif op.action == "delete":
            self.prices.delete(uuid)

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
