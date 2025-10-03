from decimal import Decimal, ROUND_HALF_DOWN, ROUND_HALF_UP
from src.crypto.mexc.client import MexcClient
from src.crypto.kucoin.client import KucoinClient
from src.model import ExchangeClient, OrderLevel, INVENTORY_BALANCE, INVENTORY_LIMIT, MEXC_TICK_SIZE, OrderBook


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


from decimal import Decimal
from collections import defaultdict


def subtract_orderbooks(main_orderbook, subtract_orderbook):
    # Create dictionaries to aggregate sizes by price
    ask_sizes = defaultdict(Decimal)
    bid_sizes = defaultdict(Decimal)

    # Add all orders from the main orderbook
    for level in main_orderbook.asks:
        ask_sizes[level.price] += level.size
    for level in main_orderbook.bids:
        bid_sizes[level.price] += level.size

    # Subtract orders from the subtract orderbook
    for level in subtract_orderbook.asks:
        ask_sizes[level.price] -= level.size
    for level in subtract_orderbook.bids:
        bid_sizes[level.price] -= level.size

    # Rebuild order lists, filtering out zero/negative sizes and keeping original structure
    result_asks = []
    result_bids = []

    # Process asks (should be sorted by price ascending)
    for price in sorted(ask_sizes.keys()):
        size = ask_sizes[price]
        if size > 0:
            # Use the ID from main orderbook if available, otherwise empty
            original_level = next((l for l in main_orderbook.asks if l.price == price), None)
            id_value = original_level.id if original_level else ''
            result_asks.append(OrderLevel(id=id_value, price=price, size=size))

    # Process bids (should be sorted by price descending)
    for price in sorted(bid_sizes.keys(), reverse=True):
        size = bid_sizes[price]
        if size > 0:
            original_level = next((l for l in main_orderbook.bids if l.price == price), None)
            id_value = original_level.id if original_level else ''
            result_bids.append(OrderLevel(id=id_value, price=price, size=size))

    return OrderBook(asks=result_asks, bids=result_bids)


from decimal import Decimal


def calculate_real_fair_price(orderbook):
    bids = orderbook.bids  # Already sorted: highest bid first
    asks = orderbook.asks  # Already sorted: lowest ask first

    # Use the minimum number of levels available
    N = min(len(bids), len(asks))

    numerator = Decimal('0')
    total_ask_volume = Decimal('0')
    total_bid_volume = Decimal('0')

    # Sum from i=1 to N: (BidPrice[i] × AskVolume[i]) + (AskPrice[i] × BidVolume[i])
    for i in range(N):
        bid_price = bids[i].price
        ask_volume = asks[i].size
        ask_price = asks[i].price
        bid_volume = bids[i].size

        numerator += (bid_price * ask_volume) + (ask_price * bid_volume)
        total_ask_volume += ask_volume
        total_bid_volume += bid_volume

    denominator = total_ask_volume + total_bid_volume

    if denominator == 0:
        return Decimal('0')

    return numerator / denominator



def calculate_fair_price(mexc_client: MexcClient, kucoin_client: KucoinClient, active_bids: list[OrderLevel], active_asks: list[OrderLevel], percent: Decimal):
    mexc_orderbook = mexc_client.get_orderbook()
    kucoin_orderbook = kucoin_client.get_orderbook()

    our_mexc_orders = mexc_client.get_active_orders()
    #print(our_mexc_orders)
    #print(mexc_orderbook)
    #print(subtract_orderbooks(mexc_orderbook, our_mexc_orders))
    real_fair_price = calculate_real_fair_price(subtract_orderbooks(mexc_orderbook, our_mexc_orders))
    print("rrrrrrrrrrrrrrrrrrrrrrr")
    print("Real MexC Fair Price: ", real_fair_price)
    print('ddddddddddddddddddddd')


    if len(mexc_orderbook.asks) == 0 or len(kucoin_orderbook.asks) == 0:
        return None

    mexc_mid_price = (mexc_orderbook.asks[0].price + mexc_orderbook.bids[0].price) / 2
    kucoin_mid_price = (kucoin_orderbook.asks[0].price + kucoin_orderbook.bids[0].price) / 2

    kucoin_liquidity = calculate_market_depth(client=kucoin_client, percent=percent)
    mexc_liquidity = calculate_market_depth(client=mexc_client, percent=percent)

    fair_price = ((mexc_mid_price * mexc_liquidity + kucoin_mid_price * kucoin_liquidity) / (mexc_liquidity + kucoin_liquidity)).quantize(Decimal('0.00001'), rounding=ROUND_HALF_UP)

    if fair_price > kucoin_orderbook.asks[0].price:
        fair_price = kucoin_orderbook.asks[0].price.quantize(Decimal('0.00001'), rounding=ROUND_HALF_DOWN)

    if fair_price < kucoin_orderbook.bids[0].price:
        fair_price = kucoin_orderbook.bids[0].price.quantize(Decimal('0.00001'), rounding=ROUND_HALF_UP)

    return fair_price, real_fair_price


def calculate_market_spread(client: ExchangeClient):
    orderbook = client.get_orderbook()

    if len(orderbook.asks) == 0 or len(orderbook.bids) == 0:
        return None

    lowest_ask = orderbook.asks[0].price
    highest_bid = orderbook.bids[0].price

    mid_price = (lowest_ask + highest_bid) / 2
    percent_spread = (lowest_ask - highest_bid) / mid_price * 100

    return percent_spread


def get_quotes(mexc_client: MexcClient, kucoin_client: KucoinClient):
    active_orders = mexc_client.get_active_orders()
    balance = mexc_client.get_balance()

    fair_price, real_fair_price = calculate_fair_price(mexc_client=mexc_client, kucoin_client=kucoin_client, active_asks=active_orders.asks, active_bids=active_orders.bids, percent=Decimal('2'))

    if fair_price is None or 'RMV' not in balance or 'USDT' not in balance:
        return None, None

    current_inventory = balance['RMV']['free'] + balance['RMV']['locked']
    half_spread = Decimal('0.00002')
    alpha = half_spread * Decimal('0.5')
    normalized_inventory_position = (current_inventory - INVENTORY_BALANCE) / INVENTORY_LIMIT

    ask_price = fair_price + half_spread - alpha * normalized_inventory_position
    bid_price = fair_price - half_spread - alpha * normalized_inventory_position

    ask_price = ask_price.quantize(MEXC_TICK_SIZE, rounding=ROUND_HALF_UP)
    bid_price = bid_price.quantize(MEXC_TICK_SIZE, rounding=ROUND_HALF_UP)

    return ask_price, bid_price