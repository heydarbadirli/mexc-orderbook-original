import asyncio
import os
from dotenv import load_dotenv
from decimal import Decimal, getcontext
from src.mexc.client import MexcClient
from src.model import CryptoCurrency, OrderBook
from src.kucoin.client import KucoinClient
import random
import time
from loguru import logger

load_dotenv()

getcontext().prec = 6

api_key_mexc = os.getenv("API_KEY_MEXC")
api_secret_mexc = os.getenv("API_SECRET_MEXC")

MEXC_BPS = Decimal("0.00001")

active_asks = []
active_bids = []


def calculate_market_depth(orderbook, percent: Decimal):
    if len(orderbook.asks) == 0:
        return 0

    lowest_ask = orderbook.asks[0].price
    highest_bid = orderbook.bids[0].price
    mid_price = (lowest_ask + highest_bid) / 2

    upper_bound = mid_price * (1 + percent / 100)
    lower_bound = mid_price * (1 - percent / 100)

    ask_depth = 0
    for ask in orderbook.asks:
        if ask.price <= upper_bound:
            ask_depth += int(ask.quantity)

    bid_depth = 0
    for bid in orderbook.bids:
        if bid.price >= lower_bound:
            bid_depth += int(bid.quantity)

    md = (ask_depth + bid_depth) * mid_price
    return md


def get_fair_price():
    mexc_orderbook = mexc_client.get_orderbook()
    kucoin_orderbook = kucoin_client.get_orderbook()

    if len(mexc_orderbook.asks) == 0 or len(kucoin_orderbook.asks) == 0:
        return None

    mexc_mid_price = (mexc_orderbook.asks[0].price + mexc_orderbook.bids[0].price) / 2
    kucoin_mid_price = (kucoin_orderbook.asks[0].price + kucoin_orderbook.bids[0].price) / 2

    mexc_liquidity = calculate_market_depth(orderbook=mexc_orderbook, percent=Decimal(2))
    kucoin_liquidity = calculate_market_depth(orderbook=kucoin_orderbook, percent=Decimal(2))

    fair_price = round((mexc_mid_price * mexc_liquidity + kucoin_mid_price * kucoin_liquidity) / (mexc_liquidity + kucoin_liquidity), 5)

    return fair_price


def update_orders(side: str):
    if side == 'sell':
        active_asks.pop(0)
    elif side == 'buy':
        active_bids.pop(0)


async def orderbook_change():
    mexc_orderbook = mexc_client.get_orderbook()
    kucoin_orderbook = kucoin_client.get_orderbook()

    if len(mexc_orderbook.asks) == 0 or len(kucoin_orderbook.asks) == 0:
        return

    # lowest_ask_mexc = mexc_orderbook.asks[0].price
    # highest_bid_mexc = mexc_orderbook.bids[0].price

    fair_price = get_fair_price()

    for ask in active_asks:
        if ask['price'] <= fair_price or ask['price'] > fair_price + 7 * MEXC_BPS:
            await mexc_client.cancel_order(first_currency=CryptoCurrency.RMV, second_currency=CryptoCurrency.USDT, order_id=ask['order_id'])
            active_asks.pop()

    for bid in active_bids:
        if bid['price'] >= fair_price or bid['price'] < fair_price - 7 * MEXC_BPS:
            await mexc_client.cancel_order(first_currency=CryptoCurrency.RMV, second_currency=CryptoCurrency.USDT, order_id=bid['order_id'])
            active_bids.pop()


    act_ask = fair_price + 2 * MEXC_BPS
    act_bid = fair_price - 2 * MEXC_BPS
    # print(f'fair price: {fair_price}')

    for _ in range(5):
        found = any(d['price'] == act_ask for d in active_asks)
        # print(f'found {found}')
        # print(active_asks)

        if not found:
            # print(f'ask: {act_ask}')
            sell_size = Decimal(random.randint(500, 2000))
            sell_id = await mexc_client.place_limit_order(first_currency=CryptoCurrency.RMV,second_currency=CryptoCurrency.USDT, side='sell',order_type='limit', size=sell_size, price=act_ask)

            active_asks.append({'order_id': sell_id, 'price': act_ask, 'size': sell_size})
            act_ask += MEXC_BPS

        found = any(d['price'] == act_bid for d in active_bids)
        # print(f'found {found}')
        # print(active_bids)

        if not found:
            # print(f'bid: {act_bid}')
            buy_size = Decimal(random.randint(500, 2000))
            buy_id = await mexc_client.place_limit_order(first_currency=CryptoCurrency.RMV,second_currency=CryptoCurrency.USDT, side='buy',order_type='limit', size=buy_size, price=act_bid)

            active_bids.append({'order_id': buy_id, 'price': act_bid, 'size': buy_size})
            act_bid -= MEXC_BPS

    # time.sleep(100)

    # while act_ask > fair_price and act_ask not in active_asks:
    #     size = Decimal(random.randint(400, 2000))
    #     order_id = await mexc_client.place_limit_order(first_currency=CryptoCurrency.RMV, second_currency=CryptoCurrency.USDT, side='sell', order_type='limit', size=size, price=act_ask)
    #
    #     active_asks.append({'order_id': order_id, 'price': act_ask, 'size': size})
    #     act_ask -= MEXC_BPS


    # while act_bid < fair_price and act_bid not in active_bids:
    #     size = Decimal(random.randint(400, 2000))
    #     order_id = await mexc_client.place_limit_order(first_currency=CryptoCurrency.RMV, second_currency=CryptoCurrency.USDT, side='buy', order_type='limit', size=size, price=act_bid)
    #
    #     active_bids.append({'order_id': order_id, 'price': act_bid, 'size': size})
    #     act_bid += MEXC_BPS


async def track_market_depth(percent: Decimal):
    counter  = 0
    while True:
        await asyncio.sleep(1)
        orderbook = mexc_client.get_orderbook()
        if len(orderbook.asks) == 0:
            continue

        md = calculate_market_depth(orderbook=orderbook, percent=percent)

        # if counter % 20 == 0:
        #     logger.info(f'market depth: {md}')

        if md < 40 and len(active_asks) > 0 and len(active_bids) > 0:
            how_many_to_add = 40 - md

            lowest_ask = orderbook.asks[0].price
            highest_bid = orderbook.bids[0].price
            mid_price = (lowest_ask + highest_bid) / 2

            upper_bound = mid_price * (1 + percent / 100)
            lower_bound = mid_price * (1 - percent / 100)

            ask_id = 0
            bid_id = 0

            while how_many_to_add > 0:
                print(2, how_many_to_add)
                if ask_id >= len(active_asks) or upper_bound < active_asks[ask_id]['price']:
                    ask_id = 0

                if bid_id >= len(active_bids) or lower_bound > active_bids[bid_id]['price']:
                    bid_id = 0

                if 0 <= ask_id < len(active_asks) and upper_bound >= active_asks[ask_id]['price']:
                    sell_size = Decimal(random.randint(500, 2000))
                    size = sell_size + active_asks[ask_id]['size']
                    price = active_asks[ask_id]['price']

                    await mexc_client.cancel_order(first_currency=CryptoCurrency.RMV, second_currency=CryptoCurrency.USDT, order_id=active_asks[ask_id]['order_id'])
                    order_id = await mexc_client.place_limit_order(first_currency=CryptoCurrency.RMV, second_currency=CryptoCurrency.USDT, side='sell', order_type='limit', size=size, price=price)

                    active_asks.pop(ask_id)
                    active_asks.insert(0, {'order_id': order_id, 'price': price, 'size': size})

                    ask_id += 1
                    how_many_to_add -= sell_size * price

                if 0 <= bid_id < len(active_bids) and lower_bound <= active_bids[bid_id]['price']:
                    buy_size = Decimal(random.randint(500, 2000))
                    size = buy_size + active_bids[bid_id]['size']
                    price = active_bids[bid_id]['price']

                    await mexc_client.cancel_order(first_currency=CryptoCurrency.RMV, second_currency=CryptoCurrency.USDT, order_id=active_bids[bid_id]['order_id'])
                    order_id = await mexc_client.place_limit_order(first_currency=CryptoCurrency.RMV, second_currency=CryptoCurrency.USDT, side='buy', order_type='limit', size=size, price=price)

                    active_bids.pop(bid_id)
                    active_bids.insert(0, {'order_id': order_id, 'price': price, 'size': size})

                    bid_id += 1
                    how_many_to_add -= buy_size * price

        counter += 1


async def track_spread():
    counter = 0
    while True:
        await asyncio.sleep(1)
        mexc_orderbook = mexc_client.get_orderbook()

        if len(mexc_orderbook.asks) == 0 or len(active_bids) == 0 or len(active_asks) == 0:
            continue

        lowest_ask_mexc = mexc_orderbook.asks[0].price
        highest_bid_mexc = mexc_orderbook.bids[0].price

        mid_price = (lowest_ask_mexc + highest_bid_mexc) / 2

        percent_spread = (lowest_ask_mexc - highest_bid_mexc) / mid_price * 100

        # if counter % 20 == 0:
        #     logger.info(f'percent spread: {percent_spread}')

        while percent_spread > 2:
            print(1)
            # if active_asks[-1]['price'] - MEXC_BPS > highest_bid_kucoin:
            sell_size = Decimal(random.randint(500, 600))
            sell_price = active_asks[0]['price'] - MEXC_BPS

            order_id = await mexc_client.place_limit_order(first_currency=CryptoCurrency.RMV, second_currency=CryptoCurrency.USDT, side='sell', order_type='limit', size=sell_size, price=sell_price)
            active_asks.insert(0, {'price': active_asks[0]['price'] - MEXC_BPS, 'size': sell_size, 'order_id': order_id})

            # if active_bids[-1]['price'] + MEXC_BPS < lowest_ask_kucoin:
            buy_size = Decimal(random.randint(500, 600))
            buy_price = active_bids[0]['price'] + MEXC_BPS

            order_id = await mexc_client.place_limit_order(first_currency=CryptoCurrency.RMV, second_currency=CryptoCurrency.USDT, side='buy', order_type='limit', size=buy_size, price=buy_price)
            active_bids.insert(0, {'price': active_bids[0]['price'] + MEXC_BPS, 'size': buy_size, 'order_id': order_id})

            lowest_ask_mexc = sell_price
            highest_bid_mexc = buy_price
            mid_price = (lowest_ask_mexc + highest_bid_mexc) / 2
            percent_spread = (lowest_ask_mexc - highest_bid_mexc) / mid_price * 100

        counter += 1


mexc_client = MexcClient(api_key=api_key_mexc, api_secret=api_secret_mexc, orderbook_change=orderbook_change, update_orders=update_orders)
kucoin_client = KucoinClient(orderbook_change=orderbook_change)


async def main():
    asyncio.create_task(mexc_client.update_orderbook(first_currency=CryptoCurrency.RMV, second_currency=CryptoCurrency.USDT))
    asyncio.create_task(mexc_client.update_balances())
    asyncio.create_task(kucoin_client.update_orderbook(first_currency=CryptoCurrency.RMV, second_currency=CryptoCurrency.USDT))
    asyncio.create_task(mexc_client.track_active_orders())
    asyncio.create_task(track_spread())
    asyncio.create_task(track_market_depth(Decimal(2)))

    while True:
        await asyncio.sleep(30)
        print(await mexc_client.get_balance())

    mexc_client.cancel_all_orders()


    #     print(f'active asks: {active_asks}')
    #     print(f'active bids: {active_bids}')
    # md = market_depth(percent=Decimal(2), orderbook=mexc_client.get_orderbook())

    # if md < Decimal("100"):

    # await asyncio.gather(update_orderbook_mexc_task, update_balance_mexc, update_orderbook_kucoin_task, tracking_orders_task, cancel_all_orders_task, spread_task)

    # y = asyncio.create_task(mexc_client.place_limit_order(first_currency=CryptoCurrency.RMV, second_currency=CryptoCurrency.USDT, side='sell', order_type='limit', size=Decimal(500), price=Decimal("0.00263")))
    # await asyncio.gather(x, y)

    # test = asyncio.create_task(mexc_client.update_balances())
    #
    # while True:
    #     await asyncio.sleep(5)
    #     print(await mexc_client.get_balance())



    # asyncio.create_task(mexc_client.update_orderbook(first_currency=CryptoCurrency.RMV, second_currency=CryptoCurrency.USDT))
    # asyncio.create_task(kucoin_client.update_orderbook(first_currency=CryptoCurrency.RMV, second_currency=CryptoCurrency.USDT))
    #
    # while True:
    #     await asyncio.sleep(1)
    #     print(get_fair_price())

if __name__ == '__main__':
    asyncio.run(main())