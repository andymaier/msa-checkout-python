"""Konfiguration (Defaults = Java-Original application.yml)."""
import os
from uuid import uuid4

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
SHOP_TOPIC = os.getenv("SHOP_TOPIC", "shop")
# Java: checkout-${random.uuid}
KAFKA_GROUP_ID = os.getenv("KAFKA_GROUP_ID", f"checkout-{uuid4()}")
SERVER_PORT = int(os.getenv("SERVER_PORT", "8082"))
