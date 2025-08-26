from enum import Enum, auto
from decimal import Decimal
import msgspec
from dataclasses import dataclass
from typing import Any

class CryptoCurrency(Enum):
    RMV = "RMV"
    USDT = "USDT"

class OrderLevel(msgspec.Struct):
    price: Decimal
    size: Decimal

class OrderBook(msgspec.Struct):
    asks: list[OrderLevel]
    bids: list[OrderLevel]

class ExchangeClient:
    @staticmethod
    def get_orderbook() -> OrderBook:
        ...

@dataclass
class DatabaseOrder:
    pair: str
    side: str
    price: Decimal
    size: Decimal
    timestamp: str

@dataclass
class DatabaseMarketState:
    market_depth: Decimal
    fair_price: Decimal
    market_spread: Decimal
    usdt_balance: Decimal
    rmv_balance: Decimal
    rmv_value: Decimal
    timestamp: str

class EventType(Enum):
    KUCOIN_ORDERBOOK_UPDATE = auto()
    MEXC_ORDERBOOK_UPDATE = auto()
    FILLED_ORDER = auto()

@dataclass
class QueueEvent:
    type: EventType
    data: Any