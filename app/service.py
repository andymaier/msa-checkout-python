"""Fachlogik (Pendant zu CheckoutService)."""


class CheckoutService:
    def are_articles_available(self, basket: dict) -> bool:
        # Stub wie im Java-Original: Artikel gelten immer als verfuegbar.
        return True
