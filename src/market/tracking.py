from decimal import Decimal
from src.model import CryptoCurrency, DatabaseOrder, OrderBook, OrderLevel
from src.mexc.client import MexcClient
from src.kucoin.client import KucoinClient
import random
import asyncio
from src.market.calculations import calculate_fair_price, calculate_market_depth
from src.database.client import DatabaseClient
from datetime import datetime
from loguru import logger

active_asks = []
active_bids = []

MEXC_TICK_SIZE = Decimal("0.00001")

async def reset_orders(mexc_client: MexcClient):
    global active_bids, active_asks

    while True:
        await asyncio.sleep(15 * 60)
        logger.info("Resetting orders")
        await mexc_client.cancel_all_orders()
        active_bids = []
        active_asks = []

async def record_our_orders(timestamp: str, database_client: DatabaseClient):
    bids = [OrderLevel(price=bid['price'], size=bid['size']) for bid in active_bids]
    asks = [OrderLevel(price=ask['price'], size=ask['size']) for ask in active_asks]

    temp_orderbook = OrderBook(asks=asks, bids=bids)
    await database_client.record_orderbook(table="our_orders", exchange="None", orderbook=temp_orderbook, timestamp=timestamp)

# update_list_of_active_orders:
# if we have just sold, we delete from active_asks
# if we have just bought, we delete from active_bids

async def update_list_of_active_orders(data, kucoin_client: KucoinClient, database_client: DatabaseClient):
    side = 'buy' if data['tradeType'] == 1 else 'sell'
    size = Decimal(str(data['singleDealQuantity']))
    price = Decimal(str(data['singleDealPrice']))
    kucoin_orderbook = kucoin_client.get_orderbook()
    pair = 'RMV-USDT'
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    order_id = data['id']

    order = DatabaseOrder(pair=pair, side=side, price=price, size=size, timestamp=timestamp, order_id=order_id)
    await database_client.record_order(order=order, table_name="orders")
    fee = Decimal("0.001")

    if side == 'sell':
        kucoin_lowest_ask = kucoin_orderbook.asks[0]

        if price > kucoin_lowest_ask.price * (1 + fee):
            size = min(size, kucoin_lowest_ask.size)
            profit_per_unit = (price - kucoin_lowest_ask.price * (1 + fee))
            profit = profit_per_unit * size

            logger.info(f"Arbitrage, profit: {profit}")

            # add opposite order
        if data['status'] == 2:
            active_asks.pop(0)
    elif side == 'buy':
        kucoin_highest_bid = kucoin_orderbook.bids[0]
        # logger.info(f"price: {price}")
        # logger.info(f"kucoin highest bid: {kucoin_highest_bid}")

        if price < kucoin_highest_bid.price * (1 - fee):
            size = min(size, kucoin_highest_bid.size)
            profit_per_unit = ((kucoin_highest_bid.price * (1 - fee)) - price)
            profit = profit_per_unit * size

            logger.info(f"Arbitrage, profit: {profit}")

            # add opposite order
        if data['status'] == 2:
            active_bids.pop(0)

# manage_orders:
# it calculates fair price, and basically it places orders +- two mexc tick sizes from fair price
# it takes inventory balance into account and shifts orders up or down if necessary
# if we have more than 5 active asks or bids it cancels them
# if some of our asks/bids price is too low/high it also cancels them
# it checks 5 levels of prices for bids and asks na if we don't have order on that level, we place it

async def manage_orders(mexc_client: MexcClient, kucoin_client: KucoinClient, database_client: DatabaseClient):
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

    if full_rmv_value - full_usdt_balance > 500:
        ask_shift -= MEXC_TICK_SIZE
        bid_shift -= MEXC_TICK_SIZE
    elif full_rmv_value - full_usdt_balance < -500:
        bid_shift += MEXC_TICK_SIZE
        ask_shift += MEXC_TICK_SIZE
    # if full_rmv_value - full_usdt_balance > 200:
    #     ask_shift -= MEXC_TICK_SIZE
    #     bid_shift -= MEXC_TICK_SIZE
    # elif full_rmv_value - full_usdt_balance < -200:
    #     ask_shift += MEXC_TICK_SIZE
    #     bid_shift += MEXC_TICK_SIZE

    if full_usdt_balance < full_rmv_value:
        bid_shift -= MEXC_TICK_SIZE
    elif full_rmv_value < full_usdt_balance:
        ask_shift += MEXC_TICK_SIZE

    if len(mexc_orderbook.asks) == 0 or len(kucoin_orderbook.asks) == 0:
        return

    while len(active_asks) > 5:
        await mexc_client.cancel_order(first_currency=CryptoCurrency.RMV, second_currency=CryptoCurrency.USDT, order_id=active_asks[len(active_asks) - 1]['order_id'])
        active_asks.pop()

    while len(active_bids) > 5:
        await mexc_client.cancel_order(first_currency=CryptoCurrency.RMV, second_currency=CryptoCurrency.USDT, order_id=active_bids[len(active_bids) - 1]['order_id'])
        active_bids.pop()


    while len(active_asks) > 0 and active_asks[0]['price'] <= fair_price + MEXC_TICK_SIZE + ask_shift:
        await mexc_client.cancel_order(first_currency=CryptoCurrency.RMV, second_currency=CryptoCurrency.USDT, order_id=active_asks[0]['order_id'])
        active_asks.pop(0)

    while len(active_bids) > 0 and active_bids[0]['price'] >= fair_price - MEXC_TICK_SIZE + bid_shift:
        await mexc_client.cancel_order(first_currency=CryptoCurrency.RMV, second_currency=CryptoCurrency.USDT, order_id=active_bids[0]['order_id'])
        active_bids.pop(0)


    act_ask = fair_price + 2 * MEXC_TICK_SIZE + ask_shift
    act_bid = fair_price - 2 * MEXC_TICK_SIZE + bid_shift

    for _ in range(5):
        found = any(d['price'] == act_ask for d in active_asks)

        if not found:
            sell_size = Decimal(random.randint(2_000, 5_000))

            sell_id = await mexc_client.place_limit_order(first_currency=CryptoCurrency.RMV,second_currency=CryptoCurrency.USDT, side='sell',order_type='limit', size=sell_size, price=act_ask)
            if sell_id is None:
                logger.error(f'Failed to place limit order: price: {act_ask}, size: {sell_size}')
                break
            else:
                timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                order = DatabaseOrder(pair='RMV-USDT', side='sell', price=act_ask, size=sell_size, order_id=sell_id, timestamp=timestamp)
                await database_client.record_order(order=order, table_name="every_order_placed")

                active_asks.append({'order_id': sell_id, 'price': act_ask, 'size': sell_size})

                active_asks.sort(key=lambda x: x['price'])
        act_ask += MEXC_TICK_SIZE

    for _ in range(5):
        found = any(d['price'] == act_bid for d in active_bids)

        if not found:
            buy_size = Decimal(random.randint(2_000, 5_000))

            buy_id = await mexc_client.place_limit_order(first_currency=CryptoCurrency.RMV,second_currency=CryptoCurrency.USDT, side='buy',order_type='limit', size=buy_size, price=act_bid)
            if buy_id is None:
                logger.error(f"Failed to place limit order: price: {act_bid}, size: {buy_size}")
                break
            else:
                timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                order = DatabaseOrder(pair='RMV-USDT', side='buy', price=act_bid, size=buy_size, order_id=buy_id, timestamp=timestamp)
                await database_client.record_order(order=order, table_name="every_order_placed")

                active_bids.append({'order_id': buy_id, 'price': act_bid, 'size': buy_size})

                active_bids.sort(key=lambda x: x['price'], reverse=True)
        act_bid -= MEXC_TICK_SIZE

# track_market_spread
# it just calculates market spread

async def track_market_spread(mexc_client: MexcClient):
    mexc_orderbook = mexc_client.get_orderbook()

    # if len(mexc_orderbook.asks) == 0 or len(active_bids) == 0 or len(active_asks) == 0:
    #     return -1

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

# track_market_depth
# it calculates market depth and if it is below expected_market_depth it add size to our orders
# first it add to highest asks and lowest bids and so on
# it also takes into account inventory balance and calculates ration of usdt balance and rmv balance and add more size on the side that we have more currency

async def track_market_depth(mexc_client: MexcClient, database_client: DatabaseClient, percent: Decimal, expected_market_depth: Decimal):
    # await asyncio.sleep(1)
    mexc_orderbook = mexc_client.get_orderbook()
    if len(mexc_orderbook.asks) == 0 or len(active_asks) == 0 or len(active_bids) == 0:
        return -1


    lowest_ask = mexc_orderbook.asks[0].price
    highest_bid = mexc_orderbook.bids[0].price
    mid_price = (lowest_ask + highest_bid) / 2

    upper_bound = mid_price * (1 + percent / 100)
    lower_bound = mid_price * (1 - percent / 100)

    for i in range(0, len(active_asks)):
        if active_asks[i]['price'] > upper_bound and active_asks[i]['size'] > 5_000:
            size = Decimal(random.randint(2_000, 5_000))
        elif active_asks[i]['size'] > 150_000:
            size = Decimal(random.randint(100_000, 150_000))

        if (active_asks[i]['price'] > upper_bound and active_asks[i]['size'] > 5_000) or active_asks[i]['size'] > 150_000:
            await mexc_client.cancel_order(first_currency=CryptoCurrency.RMV, second_currency=CryptoCurrency.USDT, order_id=active_asks[i]['order_id'])
            price = active_asks[i]['price']
            order_id = await mexc_client.place_limit_order(first_currency=CryptoCurrency.RMV, second_currency=CryptoCurrency.USDT, side='sell', order_type='limit', size=size, price=price)
            if order_id is not None:
                timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                order = DatabaseOrder(pair='RMV-USDT', side='sell', price=price, size=size, order_id=order_id, timestamp=timestamp)
                await database_client.record_order(order=order, table_name="every_order_placed")

                active_asks[i] = {'order_id': order_id, 'price': price, 'size': size}
            else:
                logger.error(f'Failed to place limit order: price: {price}, size: {size}')

    for i in range(0, len(active_bids)):
        if active_bids[i]['price'] < lower_bound and active_bids[i]['size'] > 5_000:
            size = Decimal(random.randint(2_000, 5_000))
        elif active_bids[i]['size'] > 150_000:
            size = Decimal(random.randint(100_000, 150_000))

        if (active_bids[i]['price'] < lower_bound and active_bids[i]['size'] > 5_000) or active_bids[i]['size'] > 150_000:
            await mexc_client.cancel_order(first_currency=CryptoCurrency.RMV, second_currency=CryptoCurrency.USDT, order_id=active_bids[i]['order_id'])
            price = active_bids[i]['price']
            order_id = await mexc_client.place_limit_order(first_currency=CryptoCurrency.RMV, second_currency=CryptoCurrency.USDT, side='buy', order_type='limit', size=size, price=price)
            if order_id is not None:
                timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                order = DatabaseOrder(pair='RMV-USDT', side='buy', price=price, size=size, order_id=order_id, timestamp=timestamp)
                await database_client.record_order(order=order, table_name="every_order_placed")

                active_bids[i] = {'order_id': order_id, 'price': price, 'size': size}
            else:
                logger.error(f'Failed to place limit order: price: {price}, size: {size}')


    market_depth = calculate_market_depth(client=mexc_client, percent=percent)
    # logger.info(f"Market depth {market_depth}")

    if market_depth < expected_market_depth:
        how_many_to_add = expected_market_depth - market_depth

        usdt_balance = Decimal(str(mexc_client.get_balance()['USDT']['free']))
        rmv_balance = Decimal(str(mexc_client.get_balance()['RMV']['free']))
        rmv_value = mid_price * rmv_balance

        if rmv_value > usdt_balance:
            ratio = usdt_balance / rmv_value
            how_many_to_add_usdt = how_many_to_add * ratio
            how_many_to_add_rmv = how_many_to_add - how_many_to_add_usdt
        else:
            ratio = rmv_value / usdt_balance
            how_many_to_add_rmv = how_many_to_add * ratio
            how_many_to_add_usdt = how_many_to_add - how_many_to_add_rmv

        ask_id = len(active_asks) - 1
        stopper = 0
        while how_many_to_add_rmv > 0 and stopper < 1_000:
            # print(1)
            if ask_id < 0: # change something with this 1
                ask_id = len(active_asks) - 1

            if 0 <= ask_id and upper_bound >= active_asks[ask_id]['price']:
                sell_size = Decimal(random.randint(2_000, 5_000))
                size = sell_size + active_asks[ask_id]['size']
                price = active_asks[ask_id]['price']

                await mexc_client.cancel_order(first_currency=CryptoCurrency.RMV, second_currency=CryptoCurrency.USDT, order_id=active_asks[ask_id]['order_id'])
                order_id = await mexc_client.place_limit_order(first_currency=CryptoCurrency.RMV, second_currency=CryptoCurrency.USDT, side='sell', order_type='limit', size=size, price=price)
                if order_id is None:
                    logger.error(f'Failed to place limit order: price: {price}, size: {size}')
                    break
                else:
                    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                    order = DatabaseOrder(pair='RMV-USDT', side='sell', price=price, size=size, order_id=order_id, timestamp=timestamp)
                    await database_client.record_order(order=order, table_name="every_order_placed")

                    active_asks[ask_id] = {'order_id': order_id, 'price': price, 'size': size}
                    how_many_to_add_rmv -= sell_size * price

            ask_id -= 1
            stopper += 1

        bid_id = len(active_bids) - 1
        stopper = 0
        while how_many_to_add_usdt > 0 and stopper < 1_000:
            # print(2)
            if bid_id < 0:
                bid_id = len(active_bids) - 1

            if 0 <= bid_id and lower_bound <= active_bids[bid_id]['price']:
                buy_size = Decimal(random.randint(2_000, 5_000))

                size = buy_size + active_bids[bid_id]['size']
                price = active_bids[bid_id]['price']

                await mexc_client.cancel_order(first_currency=CryptoCurrency.RMV, second_currency=CryptoCurrency.USDT, order_id=active_bids[bid_id]['order_id'])
                order_id = await mexc_client.place_limit_order(first_currency=CryptoCurrency.RMV, second_currency=CryptoCurrency.USDT, side='buy', order_type='limit', size=size, price=price)
                if order_id is None:
                    logger.error(f'Failed to place limit order: price: {price}, size: {size}')
                    break
                else:
                    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                    order = DatabaseOrder(pair='RMV-USDT', side='buy', price=price, size=size, order_id=order_id, timestamp=timestamp)
                    await database_client.record_order(order=order, table_name="every_order_placed")

                    active_bids[bid_id] = {'order_id': order_id, 'price': price, 'size': size}
                    how_many_to_add_usdt -= buy_size * price

            bid_id -= 1
            stopper += 1

    return market_depth