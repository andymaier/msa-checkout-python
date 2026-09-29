"""REST-API (Flask) - Loesung."""
from uuid import uuid4

from flask import Blueprint, request, jsonify

from .service import CheckoutService
from .store import prices
from .events import ShopProducer, Operation
from .errors import NoPriceException

bp = Blueprint("checkout", __name__)
service = CheckoutService()
producer = ShopProducer()


@bp.post("/checkouts")
def checkout():
    basket = request.get_json(force=True)

    if not service.are_articles_available(basket):
        return "", 409

    uuid = str(uuid4())
    basket["uuid"] = uuid

    for item in basket.get("items", []):
        price = prices.get(item.get("articleId"))
        if price is None:
            raise NoPriceException()
        item["price"] = price

    try:
        producer.send(Operation("basket", "upsert", basket))
    except Exception:
        return jsonify({"error": "Can not send message to eventbus!"}), 503

    return jsonify({"uuid": uuid}), 202
