from decimal import Decimal
from src.model import CryptoCurrency
from src.mexc.client import MexcClient
from src.kucoin.client import KucoinClient
import random
import asyncio
from src.market.calculations import calculate_fair_price, calculate_market_depth
from loguru import logger

active_asks = []
active_bids = []

MEXC_TICK_SIZE = Decimal("0.00001")

def update_active_orders(side: str):
    if side == 'sell':
        active_asks.pop(0)
    elif side == 'buy':
        active_bids.pop(0)


async def add_fair_orders(mexc_client: MexcClient, kucoin_client: KucoinClient):
    mexc_orderbook = mexc_client.get_orderbook()
    kucoin_orderbook = kucoin_client.get_orderbook()

    if len(mexc_orderbook.asks) == 0 or len(kucoin_orderbook.asks) == 0:
        return

    while len(active_asks) > 5:
        await mexc_client.cancel_order(first_currency=CryptoCurrency.RMV, second_currency=CryptoCurrency.USDT, order_id=active_asks[len(active_asks) - 1]['order_id'])
        active_asks.pop(0)

    while len(active_bids) > 5:
        await mexc_client.cancel_order(first_currency=CryptoCurrency.RMV, second_currency=CryptoCurrency.USDT, order_id=active_bids[len(active_bids) - 1]['order_id'])
        active_bids.pop(0)


    fair_price = calculate_fair_price(mexc_client=mexc_client, kucoin_client=kucoin_client, active_asks=active_asks, active_bids=active_bids, percent=Decimal(2))

    for i in range(len(active_asks)-1, -1, -1):
        ask = active_asks[i]
        if ask['price'] <= fair_price + MEXC_TICK_SIZE:
            await mexc_client.cancel_order(first_currency=CryptoCurrency.RMV, second_currency=CryptoCurrency.USDT, order_id=ask['order_id'])
            del active_asks[i]

    for i in range(len(active_bids)-1, -1, -1):
        bid = active_bids[i]
        if bid['price'] >= fair_price - MEXC_TICK_SIZE:
            await mexc_client.cancel_order(first_currency=CryptoCurrency.RMV, second_currency=CryptoCurrency.USDT, order_id=bid['order_id'])
            del active_bids[i]

    act_ask = fair_price + 2 * MEXC_TICK_SIZE
    act_bid = fair_price - 2 * MEXC_TICK_SIZE

    for _ in range(5):
        found = any(d['price'] == act_ask for d in active_asks)
        # print(f'FOUND {found}, ASKS: {active_asks}')

        if not found:
            sell_size = Decimal(random.randint(500, 2000))
            # print(f'FOUND {found}, ASKS: {active_asks}')
            # print(f"placing fair_order ask: {found}, {act_ask}")
            sell_id = await mexc_client.place_limit_order(first_currency=CryptoCurrency.RMV,second_currency=CryptoCurrency.USDT, side='sell',order_type='limit', size=sell_size, price=act_ask)

            active_asks.append({'order_id': sell_id, 'price': act_ask, 'size': sell_size})
            act_ask += MEXC_TICK_SIZE

        found = any(d['price'] == act_bid for d in active_bids)
        # print(f'FOUND {found}, BIDS: {active_bids}')

        if not found:
            buy_size = Decimal(random.randint(500, 2000))
            # print(f'FOUND {found}, BIDS: {active_bids}')
            # print(f"placing fair_order bid: {found}, {act_bid}")
            buy_id = await mexc_client.place_limit_order(first_currency=CryptoCurrency.RMV,second_currency=CryptoCurrency.USDT, side='buy',order_type='limit', size=buy_size, price=act_bid)

            active_bids.append({'order_id': buy_id, 'price': act_bid, 'size': buy_size})
            act_bid -= MEXC_TICK_SIZE


async def track_market_spread(mexc_client: MexcClient):
    mexc_orderbook = mexc_client.get_orderbook()

    if len(mexc_orderbook.asks) == 0 or len(active_bids) == 0 or len(active_asks) == 0:
        return -1

    lowest_ask_mexc = mexc_orderbook.asks[0].price
    highest_bid_mexc = mexc_orderbook.bids[0].price

    mid_price = (lowest_ask_mexc + highest_bid_mexc) / 2

    percent_spread = (lowest_ask_mexc - highest_bid_mexc) / mid_price * 100

    # while percent_spread > 2:
    #     sell_size = Decimal(random.randint(500, 2000))
    #     sell_price = active_asks[0]['price'] - MEXC_TICK_SIZE
    #
    #     logger.info(f'placing order to reduce spread: {sell_size}, {sell_price}')
    #     order_id = await mexc_client.place_limit_order(first_currency=CryptoCurrency.RMV, second_currency=CryptoCurrency.USDT, side='sell', order_type='limit', size=sell_size, price=sell_price)
    #     active_asks.insert(0, {'price': active_asks[0]['price'] - MEXC_TICK_SIZE, 'size': sell_size, 'order_id': order_id})
    #
    #
    #
    #     buy_size = Decimal(random.randint(500, 2000))
    #     buy_price = active_bids[0]['price'] + MEXC_TICK_SIZE
    #
    #     logger.info(f'placing order to reduce spread: {sell_size}, {sell_price}')
    #     order_id = await mexc_client.place_limit_order(first_currency=CryptoCurrency.RMV, second_currency=CryptoCurrency.USDT, side='buy', order_type='limit', size=buy_size, price=buy_price)
    #     active_bids.insert(0, {'price': active_bids[0]['price'] + MEXC_TICK_SIZE, 'size': buy_size, 'order_id': order_id})
    #
    #     lowest_ask_mexc = sell_price
    #     highest_bid_mexc = buy_price
    #     mid_price = (lowest_ask_mexc + highest_bid_mexc) / 2
    #     percent_spread = (lowest_ask_mexc - highest_bid_mexc) / mid_price * 100
    return percent_spread


async def track_market_depth(mexc_client: MexcClient, percent: Decimal, expected_market_depth: Decimal):
    # await asyncio.sleep(1)
    mexc_orderbook = mexc_client.get_orderbook()
    if len(mexc_orderbook.asks) == 0 or len(active_asks) == 0 or len(active_bids) == 0:
        return -1

    market_depth = calculate_market_depth(client=mexc_client, percent=percent)
    # print(f'MARKET: {market_depth}')

    if market_depth < expected_market_depth:
        how_many_to_add = expected_market_depth - market_depth
        # print(how_many_to_add)

        lowest_ask = mexc_orderbook.asks[0].price
        highest_bid = mexc_orderbook.bids[0].price
        mid_price = (lowest_ask + highest_bid) / 2

        upper_bound = mid_price * (1 + percent / 100)
        lower_bound = mid_price * (1 - percent / 100)

        ask_id = len(active_asks) - 1
        bid_id = len(active_bids) - 1

        usdt_balance = Decimal(str(mexc_client.get_balance()['USDT']))
        rmv_balance = Decimal(str(mexc_client.get_balance()['RMV']))
        rmv_value = mid_price * rmv_balance

        while how_many_to_add > 0:
            # print(2, how_many_to_add)
            if ask_id < 0:
                ask_id = len(active_asks) - 1

            if bid_id < 0:
                bid_id = len(active_bids) - 1

            if 0 <= ask_id < len(active_asks) and upper_bound >= active_asks[ask_id]['price'] and usdt_balance < rmv_value:
                sell_size = Decimal(random.randint(500, 2000))
                size = sell_size + active_asks[ask_id]['size']
                price = active_asks[ask_id]['price']

                # print(f'adding volume to market depth: {size}, {price}')
                await mexc_client.cancel_order(first_currency=CryptoCurrency.RMV, second_currency=CryptoCurrency.USDT, order_id=active_asks[ask_id]['order_id'])
                order_id = await mexc_client.place_limit_order(first_currency=CryptoCurrency.RMV, second_currency=CryptoCurrency.USDT, side='sell', order_type='limit', size=size, price=price)

                active_asks[ask_id] = {'order_id': order_id, 'price': price, 'size': size}

                how_many_to_add -= sell_size * price
            ask_id -= 1

            if 0 <= bid_id < len(active_bids) and lower_bound <= active_bids[bid_id]['price'] and usdt_balance > rmv_value:
                buy_size = Decimal(random.randint(500, 2000))
                size = buy_size + active_bids[bid_id]['size']
                price = active_bids[bid_id]['price']
                # print(f'adding volume to market depth: {size}, {price}')
                await mexc_client.cancel_order(first_currency=CryptoCurrency.RMV, second_currency=CryptoCurrency.USDT, order_id=active_bids[bid_id]['order_id'])
                order_id = await mexc_client.place_limit_order(first_currency=CryptoCurrency.RMV, second_currency=CryptoCurrency.USDT, side='buy', order_type='limit', size=size, price=price)

                active_bids[bid_id] = {'order_id': order_id, 'price': price, 'size': size}

                how_many_to_add -= buy_size * price
            bid_id -= 1
    return market_depth