"""Einstiegspunkt: Flask-App + Fehlerbehandlung + Kafka-Consumer-Thread."""
from flask import Flask, jsonify

from .config import SERVER_PORT
from .events import ShopListener
from .api import bp
from .errors import NoPriceException


def create_app() -> Flask:
    app = Flask(__name__)
    app.register_blueprint(bp)

    @app.errorhandler(NoPriceException)
    def handle_no_price(e):
        return jsonify({"error": "Article with no price!"}), 400

    ShopListener().start()
    return app


app = create_app()

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=SERVER_PORT)
