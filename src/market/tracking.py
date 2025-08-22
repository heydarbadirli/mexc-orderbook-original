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

order_lock = asyncio.Lock()

amount_bought = 0
amount_sold = 0

def update_active_orders(data, kucoin_client: KucoinClient):
    side = 'buy' if data['tradeType'] == 1 else 'sell'
    size = Decimal(str(data['singleDealQuantity']))
    price = Decimal(str(data['singleDealPrice']))
    kucoin_orderbook = kucoin_client.get_orderbook()

    if side == 'sell':
        kucoin_lowest_ask = kucoin_orderbook.asks[0]

        # if price > kucoin_lowest_ask.price * Decimal('1.001'):
        size = min(size, kucoin_lowest_ask.size)
        profit = (price - kucoin_lowest_ask.price * Decimal('1.001')) * size

        logger.info(f"Arbitrage, profit: {profit}")

        if data['status'] == 2:
            active_asks.pop(0)
    elif side == 'buy':
        kucoin_highest_bid = kucoin_orderbook.bids[0]

        # if price < kucoin_highest_bid.price * Decimal('1.001'):
        size = min(size, kucoin_highest_bid.size)
        profit = (price - kucoin_highest_bid.price * Decimal('1.001')) * size

        logger.info(f"Arbitrage, profit: {profit}")

        if data['status'] == 2:
            active_bids.pop(0)


async def manage_orders(mexc_client: MexcClient, kucoin_client: KucoinClient):
    mexc_orderbook = mexc_client.get_orderbook()
    kucoin_orderbook = kucoin_client.get_orderbook()

    fair_price = calculate_fair_price(mexc_client=mexc_client, kucoin_client=kucoin_client, active_asks=active_asks, active_bids=active_bids, percent=Decimal(2))
    if fair_price is None:
        return
    balances = mexc_client.get_balance()
    full_usdt_balance = balances['USDT']['free'] + balances['USDT']['locked']
    full_rmv_value = (balances['RMV']['free'] + balances['RMV']['locked']) * fair_price
    ask_shift =0
    bid_shift = 0

    if full_rmv_value - full_usdt_balance > 100:
        ask_shift -= MEXC_TICK_SIZE
    elif full_rmv_value - full_usdt_balance < -100:
        bid_shift += MEXC_TICK_SIZE


    if len(mexc_orderbook.asks) == 0 or len(kucoin_orderbook.asks) == 0:
        return

    while len(active_asks) > 5:
        await mexc_client.cancel_order(first_currency=CryptoCurrency.RMV, second_currency=CryptoCurrency.USDT, order_id=active_asks[len(active_asks) - 1]['order_id'])
        active_asks.pop()

    while len(active_bids) > 5:
        await mexc_client.cancel_order(first_currency=CryptoCurrency.RMV, second_currency=CryptoCurrency.USDT, order_id=active_bids[len(active_bids) - 1]['order_id'])
        active_bids.pop()


    for i in range(len(active_asks)-1, -1, -1):
        ask = active_asks[i]
        if ask['price'] <= fair_price + MEXC_TICK_SIZE + ask_shift:
            await mexc_client.cancel_order(first_currency=CryptoCurrency.RMV, second_currency=CryptoCurrency.USDT, order_id=ask['order_id'])
            # logger.info(f'removing {i} elements from asks, size: {len(active_asks)}')
            del active_asks[i]

    for i in range(len(active_bids)-1, -1, -1):
        bid = active_bids[i]
        if bid['price'] >= fair_price - MEXC_TICK_SIZE + bid_shift:
            await mexc_client.cancel_order(first_currency=CryptoCurrency.RMV, second_currency=CryptoCurrency.USDT, order_id=bid['order_id'])
            # logger.info(f'removing {i} elements from bids, size: {len(active_bids)}')
            del active_bids[i]


    act_ask = fair_price + 2 * MEXC_TICK_SIZE + ask_shift
    act_bid = fair_price - 2 * MEXC_TICK_SIZE + bid_shift

    for _ in range(5):
        found = any(d['price'] == act_ask for d in active_asks)
        # print(f'FOUND {found}, ASKS: {active_asks}')

        if not found:
            sell_size = Decimal(random.randint(500, 2000))
            # print(f'FOUND {found}, ASKS: {active_asks}')
            # print(f"placing fair_order ask: {found}, {act_ask}")

            sell_id = await mexc_client.place_limit_order(first_currency=CryptoCurrency.RMV,second_currency=CryptoCurrency.USDT, side='sell',order_type='limit', size=sell_size, price=act_ask)
            if sell_id is None:
                continue

            active_asks.append({'order_id': sell_id, 'price': act_ask, 'size': sell_size})
            act_ask += MEXC_TICK_SIZE

        found = any(d['price'] == act_bid for d in active_bids)
        # print(f'FOUND {found}, BIDS: {active_bids}')

        if not found:
            buy_size = Decimal(random.randint(500, 2000))
            # print(f'FOUND {found}, BIDS: {active_bids}')
            # print(f"placing fair_order bid: {found}, {act_bid}")
            buy_id = await mexc_client.place_limit_order(first_currency=CryptoCurrency.RMV,second_currency=CryptoCurrency.USDT, side='buy',order_type='limit', size=buy_size, price=act_bid)
            if buy_id is None:
                continue

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


async def track_market_depth(mexc_client: MexcClient, kucoin_client: KucoinClient, percent: Decimal, expected_market_depth: Decimal):
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

        usdt_balance = Decimal(str(mexc_client.get_balance()['USDT']['free']))
        rmv_balance = Decimal(str(mexc_client.get_balance()['RMV']['free']))
        rmv_value = mid_price * rmv_balance
        # print(usdt_balance, rmv_value)

        if rmv_value > usdt_balance:
            ratio = usdt_balance / rmv_value
            how_many_to_add_usdt = how_many_to_add * ratio
            how_many_to_add_rmv = how_many_to_add - how_many_to_add_usdt
        else:
            ratio = rmv_value / usdt_balance
            how_many_to_add_rmv = how_many_to_add * ratio
            how_many_to_add_usdt = how_many_to_add - how_many_to_add_rmv

        # print(ratio)
        ask_id = len(active_asks) - 1
        bid_id = len(active_bids) - 1

        stopper = 0
        while how_many_to_add_rmv > 0 and stopper < 1000:
            # print(1)
            if ask_id < 1:
                ask_id = len(active_asks) - 1

            if 1 <= ask_id < len(active_asks) and upper_bound >= active_asks[ask_id]['price']:
                sell_size = Decimal(random.randint(500, 2000))
                size = sell_size + active_asks[ask_id]['size']
                price = active_asks[ask_id]['price']

                # print(f'adding rmv for sale')
                await mexc_client.cancel_order(first_currency=CryptoCurrency.RMV, second_currency=CryptoCurrency.USDT, order_id=active_asks[ask_id]['order_id'])
                order_id = await mexc_client.place_limit_order(first_currency=CryptoCurrency.RMV, second_currency=CryptoCurrency.USDT, side='sell', order_type='limit', size=size, price=price)
                if order_id is None:
                    break
                # logger.info(f'replacing {ask_id} element, size: {len(active_asks)}')
                active_asks[ask_id] = {'order_id': order_id, 'price': price, 'size': size}

                how_many_to_add_rmv -= sell_size * price
            ask_id -= 1
            stopper += 1

        stopper = 0
        while how_many_to_add_usdt > 0:
            # print(2)
            if bid_id < 1:
                bid_id = len(active_bids) - 1

            if 1 <= bid_id < len(active_bids) and lower_bound <= active_bids[bid_id]['price']:
                buy_size = Decimal(random.randint(500, 2000))
                size = buy_size + active_bids[bid_id]['size']
                price = active_bids[bid_id]['price']
                # print(f'adding usdt to buy')
                await mexc_client.cancel_order(first_currency=CryptoCurrency.RMV, second_currency=CryptoCurrency.USDT, order_id=active_bids[bid_id]['order_id'])
                order_id = await mexc_client.place_limit_order(first_currency=CryptoCurrency.RMV, second_currency=CryptoCurrency.USDT, side='buy', order_type='limit', size=size, price=price)
                if order_id is None:
                    break
                # logger.info(f'replacing {bid_id} element, size: {len(active_bids)}')
                active_bids[bid_id] = {'order_id': order_id, 'price': price, 'size': size}

                how_many_to_add_usdt -= buy_size * price
            bid_id -= 1
            stopper += 1

    return market_depth