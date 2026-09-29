"""REST-API (Flask).

TODO (Uebung): POST /checkouts implementieren.
"""
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
    # TODO:
    #   1. Warenkorb (Basket) aus dem JSON-Request-Body lesen
    #   2. Verfuegbarkeit pruefen (service.are_articles_available) -> sonst 409
    #   3. uuid vergeben (basket["uuid"]); je Item Preis aus `prices` setzen,
    #      fehlt der Preis -> NoPriceException (wird zu 400)
    #   4. Operation("basket", "upsert", basket) an Topic "shop" senden
    #      (producer.send(...)); bei Eventbus-Fehler -> 503
    #   5. 202 Accepted mit {"uuid": uuid} zurueckgeben
    raise NotImplementedError("POST /checkouts noch nicht implementiert")
