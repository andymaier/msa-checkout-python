"""Datenmodelle (dataclasses, zur Orientierung)."""
from dataclasses import dataclass, field
from typing import List, Optional


@dataclass
class Item:
    articleId: str
    quantity: int = 0
    price: Optional[float] = None


@dataclass
class Basket:
    uuid: Optional[str] = None
    customer: Optional[str] = None
    items: List[Item] = field(default_factory=list)


@dataclass
class BasketIdentifier:
    uuid: str


@dataclass
class Price:
    uuid: str
    price: Optional[float] = None
