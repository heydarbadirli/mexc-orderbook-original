from decimal import Decimal, ROUND_DOWN
from src.model import CryptoCurrency, DatabaseOrder, OrderBook, OrderLevel, ExchangeClient
from src.crypto.mexc.client import MexcClient
from src.crypto.kucoin.client import KucoinClient
import random
import asyncio
from src.crypto.market.calculations import calculate_fair_price, calculate_market_depth
from src.database.client import DatabaseClient
from datetime import datetime
from loguru import logger

# active_asks: list[OrderLevel] = []
# active_bids: list[OrderLevel] = []

MEXC_TICK_SIZE = Decimal('0.00001')

INVENTORY_BALANCE = Decimal(275000)

async def reset_orders(mexc_client: MexcClient):
    # global active_bids, active_asks

    while True:
        await asyncio.sleep(60 * 60)
        logger.info("Resetting orders")
        await mexc_client.cancel_all_orders()
        # active_bids = []
        # active_asks = []

async def record_our_orders(timestamp: str, mexc_client: MexcClient, database_client: DatabaseClient):
    active_orders = mexc_client.get_active_orders()
    temp_orderbook = OrderBook(asks=active_orders.asks, bids=active_orders.bids)
    await database_client.record_orderbook(table="our_orders", exchange="None", orderbook=temp_orderbook, timestamp=timestamp)

# update_list_of_active_orders:
# if we have just sold, we delete from active_asks
# if we have just bought, we delete from active_bids

async def update_list_of_active_orders(data, database_client: DatabaseClient):
    side = 'buy' if data['tradeType'] == 1 else 'sell'
    size = Decimal(str(data['cumulativeQuantity']))
    price = Decimal(str(data['price']))
    pair = 'RMV-USDT'
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    order_id = data['id']

    order = DatabaseOrder(pair=pair, side=side, price=price, size=size, timestamp=timestamp, order_id=order_id)
    await database_client.record_order(order=order, table_name="orders")

    # if side == 'sell' and data['status'] == 2:
    #     active_asks.pop(0)
    # elif side == 'buy' and data['status'] == 2:
    #     active_bids.pop(0)

# manage_orders:
# it calculates fair price, and basically it places orders +- two mexc tick sizes from fair price
# it takes inventory balance into account and shifts orders up or down if necessary
# if we have more than 5 active asks or bids it cancels them
# if some of our asks/bids price is too low/high it also cancels them
# it checks 5 levels of prices for bids and asks na if we don't have order on that level, we place it

async def manage_orders(mexc_client: MexcClient, kucoin_client: KucoinClient, database_client: DatabaseClient):
    mexc_orderbook = mexc_client.get_orderbook()
    kucoin_orderbook = kucoin_client.get_orderbook()

    active_orders = mexc_client.get_active_orders()

    fair_price = calculate_fair_price(mexc_client=mexc_client, kucoin_client=kucoin_client, active_asks=active_orders.asks, active_bids=active_orders.bids, percent=Decimal(2))
    if fair_price is None:
        return
    balances = mexc_client.get_balance()
    full_usdt_balance = balances['USDT']['free'] + balances['USDT']['locked']
    full_rmv_balance = balances['RMV']['free'] + balances['RMV']['locked']
    full_rmv_value = (balances['RMV']['free'] + balances['RMV']['locked']) * fair_price
    ask_shift = 0
    bid_shift = 0

    if full_rmv_balance < INVENTORY_BALANCE:
        ask_shift += MEXC_TICK_SIZE
    else:
        bid_shift -= MEXC_TICK_SIZE

    if full_rmv_balance - INVENTORY_BALANCE > 37_500: # we are long
        ask_shift -= MEXC_TICK_SIZE
        bid_shift -= MEXC_TICK_SIZE
    elif full_rmv_balance - INVENTORY_BALANCE < -37_500: # we are short
        bid_shift += MEXC_TICK_SIZE
        ask_shift += MEXC_TICK_SIZE
    elif full_rmv_balance - INVENTORY_BALANCE > 75_000: # we are long
        ask_shift -= 2 * MEXC_TICK_SIZE
        bid_shift -= 2 * MEXC_TICK_SIZE
    elif full_rmv_balance - INVENTORY_BALANCE < -75_000: # we are short
        bid_shift += 2 * MEXC_TICK_SIZE
        ask_shift += 2 * MEXC_TICK_SIZE
    elif full_rmv_balance - INVENTORY_BALANCE > 150_00:
        ask_shift -= 3 * MEXC_TICK_SIZE
        bid_shift -= 3 * MEXC_TICK_SIZE
    elif full_rmv_balance - INVENTORY_BALANCE < -150_00:
        ask_shift += 3 * MEXC_TICK_SIZE
        bid_shift += 3 * MEXC_TICK_SIZE


    if len(mexc_orderbook.asks) == 0 or len(mexc_orderbook.bids) == 0 or len(kucoin_orderbook.asks) == 0 or len(kucoin_orderbook.bids) == 0:
        return

    while len(active_orders.asks) > 0 and active_orders.asks[0].price <= fair_price + 1 * MEXC_TICK_SIZE + ask_shift:
        logger.info(f'Cancelled, ask price to low: {active_orders.asks[0]}')
        await mexc_client.cancel_order(first_currency=CryptoCurrency.RMV, second_currency=CryptoCurrency.USDT, order_id=active_orders.asks[0].id)
        await asyncio.sleep(0.1)

    while len(active_orders.bids) > 0 and active_orders.bids[0].price >= fair_price - 1 * MEXC_TICK_SIZE + bid_shift:
        logger.info(f'Cancelled, bid price to high: {active_orders.bids[0]}')
        await mexc_client.cancel_order(first_currency=CryptoCurrency.RMV, second_currency=CryptoCurrency.USDT, order_id=active_orders.bids[0].id)
        await asyncio.sleep(0.1)


    act_ask = fair_price + 2 * MEXC_TICK_SIZE + ask_shift # there was 2
    act_bid = fair_price - 2 * MEXC_TICK_SIZE + bid_shift # there was 2
    logger.info(f'act_ask: {act_ask}, act_bid: {act_bid}, fair_price: {fair_price}, ask_shift: {ask_shift}, bid_shift: {bid_shift}')

    if len(active_orders.asks) > 0 and active_orders.asks[0].price == act_ask and active_orders.asks[0].size > Decimal('10000'):
        price = act_ask
        size = Decimal(random.randint(5_000, 10_000))
        await mexc_client.cancel_order(first_currency=CryptoCurrency.RMV, second_currency=CryptoCurrency.USDT,order_id=active_orders.asks[0].id)
        await asyncio.sleep(0.1)

        await mexc_client.place_limit_order(first_currency=CryptoCurrency.RMV, second_currency=CryptoCurrency.USDT,side='sell', order_type='limit', price=price, size=size)

    if len(active_orders.bids) > 0 and active_orders.bids[0].price == act_bid and active_orders.bids[0].size > Decimal('10000'):
        price = act_bid
        size = Decimal(random.randint(5_000, 10_000))
        await mexc_client.cancel_order(first_currency=CryptoCurrency.RMV, second_currency=CryptoCurrency.USDT,order_id=active_orders.bids[0].id)
        await asyncio.sleep(0.1)

        await mexc_client.place_limit_order(first_currency=CryptoCurrency.RMV, second_currency=CryptoCurrency.USDT,side='buy', order_type='limit', price=price, size=size)

    while len(active_orders.asks) > 0 and active_orders.asks[len(active_orders.asks) - 1].price >= act_ask + 5 * MEXC_TICK_SIZE:
        logger.info(f'Cancelled, ask price to high: {active_orders.asks[len(active_orders.asks) - 1]}')
        await mexc_client.cancel_order(first_currency=CryptoCurrency.RMV, second_currency=CryptoCurrency.USDT, order_id=active_orders.asks[len(active_orders.asks) - 1].id)
        await asyncio.sleep(0.1)

    while len(active_orders.bids) > 0 and active_orders.bids[len(active_orders.bids) - 1].price <= act_bid - 5 * MEXC_TICK_SIZE:
        logger.info(f'Cancelled, bid price to low: {active_orders.bids[len(active_orders.bids) - 1]}')
        await mexc_client.cancel_order(first_currency=CryptoCurrency.RMV, second_currency=CryptoCurrency.USDT, order_id=active_orders.bids[len(active_orders.bids) - 1].id)
        await asyncio.sleep(0.1)

    for _ in range(5):
        found = any(d.price == act_ask for d in active_orders.asks)
        # logger.info(f'found {found}, act_ask: {act_ask}')
        if not found:
            balances = mexc_client.get_balance()
            size = Decimal(min(random.randint(8_000, 10_000), balances['RMV']['free'] * Decimal('0.999')))
            size = size.quantize(Decimal('1'), rounding=ROUND_DOWN)

            if size <= 0 or balances['RMV']['free'] <= 400: # order value can't be less than 1 USDT
                logger.error('To small balance')
                break

            sell_id = await mexc_client.place_limit_order(first_currency=CryptoCurrency.RMV,second_currency=CryptoCurrency.USDT, side='sell',order_type='limit', size=size, price=act_ask)

            if sell_id is None:
                logger.error(f'Failed to place limit order: price: {act_ask}, size: {size}, balance: {balances}')
                break
            else:
                timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                order = DatabaseOrder(pair='RMV-USDT', side='sell', price=act_ask, size=size, order_id=sell_id, timestamp=timestamp)
                await database_client.record_order(order=order, table_name="every_order_placed")
        act_ask += MEXC_TICK_SIZE

    for _ in range(5):
        found = any(d.price == act_bid for d in active_orders.bids)

        if not found:
            balances = mexc_client.get_balance()
            size = Decimal(min(random.randint(8_000, 10_000), balances['USDT']['free'] / act_bid * Decimal('0.999')))
            size = size.quantize(Decimal('1'), rounding=ROUND_DOWN)

            if size <= 0 or balances['USDT']['free'] <= Decimal('1.5'): # order value can't be less than 1 USDT
                logger.error('To small balance')
                break

            buy_id = await mexc_client.place_limit_order(first_currency=CryptoCurrency.RMV, second_currency=CryptoCurrency.USDT, side='buy',order_type='limit', size=size, price=act_bid)

            if buy_id is None:
                logger.error(f"Failed to place limit order: price: {act_bid}, size: {size}, balance: {balances}")
                break
            else:
                timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                order = DatabaseOrder(pair='RMV-USDT', side='buy', price=act_bid, size=size, order_id=buy_id, timestamp=timestamp)
                await database_client.record_order(order=order, table_name="every_order_placed")
        act_bid -= MEXC_TICK_SIZE

# track_market_spread
# it just calculates market spread

async def track_market_spread(client: ExchangeClient):
    orderbook = client.get_orderbook()

    # get_orderbook if len(mexc_orderbook.asks) == 0 or len(active_bids) == 0 or len(active_asks) == 0:
    #     return -1

    lowest_ask = orderbook.asks[0].price
    highest_bid = orderbook.bids[0].price

    mid_price = (lowest_ask + highest_bid) / 2

    percent_spread = (lowest_ask - highest_bid) / mid_price * 100

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
    mexc_orderbook = mexc_client.get_orderbook()
    active_orders = mexc_client.get_active_orders()

    if len(mexc_orderbook.asks) == 0 or len(active_orders.asks) == 0 or len(active_orders.bids) == 0:
        return -1

    lowest_ask = mexc_orderbook.asks[0].price
    highest_bid = mexc_orderbook.bids[0].price
    mid_price = (lowest_ask + highest_bid) / 2

    upper_bound = mid_price * (1 + percent / 100)
    lower_bound = mid_price * (1 - percent / 100)

    mexc_balance = mexc_client.get_balance()

    for i in range(len(active_orders.asks) - 1, -1, -1):
        size = Decimal(0)

        if i == 0 and active_orders.asks[i].size > 20_000:
            size = Decimal(random.randint(5_000, 10_000))
        elif active_orders.asks[i].price > upper_bound and active_orders.asks[i].size > 20_000:
            size = Decimal(random.randint(5_000, 10_000))
        elif active_orders.asks[i].size > 200_000:
            size = Decimal(random.randint(100_000, 150_000))

        if (i == 0 and active_orders.asks[i].size > 20_000) or (active_orders.asks[i].price > upper_bound and active_orders.asks[i].size > 20_000) or active_orders.asks[i].size > 200_000:
            price = active_orders.asks[i].price

            await mexc_client.cancel_order(first_currency=CryptoCurrency.RMV, second_currency=CryptoCurrency.USDT, order_id=active_orders.asks[i].id)
            await asyncio.sleep(0.1)

            size = min(size, mexc_balance['RMV']['free'] * Decimal('0.999'))
            size = size.quantize(Decimal('1'), rounding=ROUND_DOWN)

            if size <= 0 or mexc_balance['RMV']['free'] <= 400: # order value can't be less than 1 USDT
                logger.error('Something went wrong')
                break

            order_id = await mexc_client.place_limit_order(first_currency=CryptoCurrency.RMV, second_currency=CryptoCurrency.USDT, side='sell', order_type='limit', size=size, price=price)

            if order_id is not None:
                timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                order = DatabaseOrder(pair='RMV-USDT', side='sell', price=price, size=size, order_id=order_id, timestamp=timestamp)
                await database_client.record_order(order=order, table_name="every_order_placed")
            else:
                logger.error(f'Failed to place limit order: price: {price}, size: {size}')

    for i in range(len(active_orders.bids) -1, -1, -1):
        size = Decimal(0)

        if i == 0 and active_orders.bids[i].size > 20_000:
            size = Decimal(random.randint(5_000, 10_000))
        elif active_orders.bids[i].price < lower_bound and active_orders.bids[i].size > 20_000:
            size = Decimal(random.randint(5_000, 10_000))
        elif active_orders.bids[i].size > 200_000:
            size = Decimal(random.randint(100_000, 150_000))

        if (i == 0 and active_orders.bids[i].size > 20_000) and (active_orders.bids[i].price < lower_bound and active_orders.bids[i].size > 20_000) or active_orders.bids[i].size > 200_000:
            price = active_orders.bids[i].price

            await mexc_client.cancel_order(first_currency=CryptoCurrency.RMV, second_currency=CryptoCurrency.USDT, order_id=active_orders.bids[i].id)
            await asyncio.sleep(0.1)

            size = min(size, mexc_balance['USDT']['free'] / price * Decimal('0.999'))
            size = size.quantize(Decimal('1'), rounding=ROUND_DOWN)

            if size <= 0 or mexc_balance['USDT']['free'] < Decimal('1.5'): # order value can't be less than 1 USDT
                logger.error('Something went wrong')
                break

            order_id = await mexc_client.place_limit_order(first_currency=CryptoCurrency.RMV, second_currency=CryptoCurrency.USDT, side='buy', order_type='limit', size=size, price=price)

            if order_id is not None:
                timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                order = DatabaseOrder(pair='RMV-USDT', side='buy', price=price, size=size, order_id=order_id, timestamp=timestamp)
                await database_client.record_order(order=order, table_name="every_order_placed")
            else:
                logger.error(f'Failed to place limit order: price: {price}, size: {size}')

    market_depth = calculate_market_depth(client=mexc_client, percent=percent)

    if market_depth < expected_market_depth * Decimal('0.98'):
        mexc_orderbook = mexc_client.get_orderbook()
        how_many_to_add = expected_market_depth - market_depth

        usdt_balance = mexc_balance['USDT']['free']
        rmv_balance = mexc_balance['RMV']['free']
        mid_price = (mexc_orderbook.asks[0].price + mexc_orderbook.bids[0].price) / 2
        rmv_value = mid_price * rmv_balance

        total_value = usdt_balance + rmv_value

        how_many_to_add_usdt = how_many_to_add * (usdt_balance / total_value)
        how_many_to_add_rmv = how_many_to_add * (rmv_value / total_value)

        # logger.info(f'how many to add: {how_many_to_add}')
        # logger.info(f'how many to add rmv: {how_many_to_add_rmv}')
        # logger.info(f'how many to add usdt: {how_many_to_add_usdt}')
        # logger.info(f'total value: {total_value}')

        ask_id = len(active_orders.asks) - 1
        stopper = 0

        while how_many_to_add_rmv > 1 and stopper < 100:
            if mexc_balance['RMV']['free'] < 400:
                break

            if ask_id < 1:
                ask_id = len(active_orders.asks) - 1

            # if ask_id == 0 and active_orders.asks[ask_id].size > 10_000:
            #     ask_id -= 1
            #     continue

            if 1 <= ask_id and upper_bound >= active_orders.asks[ask_id].price:
                price = active_orders.asks[ask_id].price

                to_add = Decimal(min(random.randint(8_000, 10_000), mexc_balance['RMV']['free'] * Decimal('0.999')))
                size = to_add + active_orders.asks[ask_id].size
                size = size.quantize(Decimal('1'), rounding=ROUND_DOWN)

                await mexc_client.cancel_order(first_currency=CryptoCurrency.RMV, second_currency=CryptoCurrency.USDT, order_id=active_orders.asks[ask_id].id)
                await asyncio.sleep(0.1)

                order_id = await mexc_client.place_limit_order(first_currency=CryptoCurrency.RMV, second_currency=CryptoCurrency.USDT, side='sell', order_type='limit', size=size, price=price)

                if order_id is not None:
                    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                    order = DatabaseOrder(pair='RMV-USDT', side='sell', price=price, size=size, order_id=order_id, timestamp=timestamp)
                    await database_client.record_order(order=order, table_name="every_order_placed")

                    how_many_to_add_rmv -= to_add * price
                else:
                    logger.error(f'Failed to place limit order: price: {price}, size: {size}, balances: {mexc_balance}')

            ask_id -= 1
            stopper += 1

        bid_id = len(active_orders.bids) - 1
        stopper = 0

        while how_many_to_add_usdt > 1 and stopper < 100:
            if mexc_balance['USDT']['free'] < 1:
                break

            if bid_id < 1:
                bid_id = len(active_orders.bids) - 1

            # if bid_id == 0 and active_orders.bids[bid_id].size > 10_000:
            #     bid_id -= 1
            #     continue

            if 1 <= bid_id and lower_bound <= active_orders.bids[bid_id].price:
                price = active_orders.bids[bid_id].price

                to_add = Decimal(min(random.randint(8_000, 10_000), mexc_balance['USDT']['free'] / price * Decimal('0.999')))
                size = to_add + active_orders.bids[bid_id].size
                size = size.quantize(Decimal('1'), rounding=ROUND_DOWN)

                await mexc_client.cancel_order(first_currency=CryptoCurrency.RMV, second_currency=CryptoCurrency.USDT, order_id=active_orders.bids[bid_id].id)
                await asyncio.sleep(0.1)

                order_id = await mexc_client.place_limit_order(first_currency=CryptoCurrency.RMV, second_currency=CryptoCurrency.USDT, side='buy', order_type='limit', size=size, price=price)

                if order_id is not None:
                    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                    order = DatabaseOrder(pair='RMV-USDT', side='buy', price=price, size=size, order_id=order_id,timestamp=timestamp)
                    await database_client.record_order(order=order, table_name="every_order_placed")

                    how_many_to_add_usdt -= to_add * price
                else:
                    logger.error(f'Failed to place limit order: price: {price}, size: {size}, balances: {mexc_balance}')

            bid_id -= 1
            stopper += 1

    return market_depth