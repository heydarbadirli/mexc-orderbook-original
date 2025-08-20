from enum import Enum
from decimal import Decimal
import msgspec

class CryptoCurrency(Enum):
    RMV = "RMV"
    USDT = "USDT"

class OrderLevel(msgspec.Struct):
    price: Decimal
    size: Decimal

class OrderBook(msgspec.Struct):
    asks: list[OrderLevel]
    bids: list[OrderLevel]

class ExchangeClient():
    @staticmethod
    def get_orderbook() -> OrderBook:
        ...