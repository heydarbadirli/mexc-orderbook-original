from decimal import Decimal, ROUND_HALF_DOWN, ROUND_HALF_UP
from src.crypto.mexc.client import MexcClient
from src.crypto.kucoin.client import KucoinClient
from src.model import ExchangeClient, OrderLevel
from loguru import logger

# calculate_market_depth:
# calculates market depth by getting upper price and lower price and adding sizes of orders

def calculate_market_depth(client: ExchangeClient, percent: Decimal) -> Decimal:
    orderbook = client.get_orderbook()

    if len(orderbook.asks) == 0 or len(orderbook.bids) == 0:
        return Decimal('0')

    lowest_ask = orderbook.asks[0].price
    highest_bid = orderbook.bids[0].price
    mid_price = (lowest_ask + highest_bid) / 2

    upper_bound = mid_price * (1 + percent / 100)
    lower_bound = mid_price * (1 - percent / 100)
    market_depth = Decimal('0')

    for ask in orderbook.asks:
        if ask.price <= upper_bound:
            market_depth += Decimal(str(ask.size)) * Decimal(str(ask.price))
        else:
            break

    for bid in orderbook.bids:
        if bid.price >= lower_bound:
            market_depth += Decimal(str(bid.size)) * Decimal(str(bid.price))
        else:
            break

    return market_depth

# calculate_fair_price
# it calculates fair price using cross exchanges formula
# takes mid-price from mexc and kucoin and liquidity on both exchanges
# IT DOES NOT TAKE INTO ACCOUNT our orders

def calculate_fair_price(mexc_client: MexcClient, kucoin_client: KucoinClient, active_bids: list[OrderLevel], active_asks: list[OrderLevel], percent: Decimal):
    mexc_orderbook = mexc_client.get_orderbook()
    kucoin_orderbook = kucoin_client.get_orderbook()

    if len(mexc_orderbook.asks) == 0 or len(kucoin_orderbook.asks) == 0:
        return None

    mexc_lowest_ask, mexc_highest_bid = None, None

    for ask in mexc_orderbook.asks:
        found = any(d.price == ask.price and d.size == ask.size for d in active_asks)
        if not found:
            mexc_lowest_ask = ask.price
            break

    for bid in mexc_orderbook.bids:
        found = any(d.price == bid.price and d.size == bid.size for d in active_bids)
        if not found:
            mexc_highest_bid = bid.price
            break


    mexc_mid_price = (mexc_lowest_ask + mexc_highest_bid) / 2
    kucoin_mid_price = (kucoin_orderbook.asks[0].price + kucoin_orderbook.bids[0].price) / 2

    upper_bound = mexc_mid_price * Decimal(1 + percent / 100)
    lower_bound = mexc_mid_price * Decimal(1 - percent / 100)

    mexc_liquidity = 0
    kucoin_liquidity = calculate_market_depth(client=kucoin_client, percent=Decimal(2))

    for ask in mexc_orderbook.asks:
        if ask.price > upper_bound:
            break

        index = next((i for i, d in enumerate(active_asks) if d.price == ask.price), None)
        mexc_liquidity += ask.size

        if index is not None:
            mexc_liquidity -= active_asks[index].size

    for bid in mexc_orderbook.bids:
        if bid.price < lower_bound:
            break

        index = next((i for i, d in enumerate(active_bids) if d.price == bid.price), None)
        mexc_liquidity += bid.size

        if index is not None:
            mexc_liquidity -= active_bids[index].size


    fair_price = ((mexc_mid_price * mexc_liquidity + kucoin_mid_price * kucoin_liquidity) / (mexc_liquidity + kucoin_liquidity)).quantize(Decimal('0.00001'), rounding=ROUND_HALF_UP)

    if fair_price > kucoin_orderbook.asks[0].price:
        fair_price = kucoin_orderbook.asks[0].price.quantize(Decimal('0.00001'), rounding=ROUND_HALF_DOWN)

    if fair_price < kucoin_orderbook.bids[0].price:
        fair_price = kucoin_orderbook.bids[0].price.quantize(Decimal('0.00001'), rounding=ROUND_HALF_UP)

    return fair_price