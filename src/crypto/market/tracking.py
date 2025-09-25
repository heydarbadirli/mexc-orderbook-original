from decimal import Decimal, ROUND_DOWN, ROUND_HALF_UP
from src.model import CryptoCurrency, DatabaseOrder, OrderBook, OrderLevel, ExchangeClient
from src.crypto.mexc.client import MexcClient
from src.crypto.kucoin.client import KucoinClient
import random
import asyncio
from src.crypto.market.calculations import calculate_fair_price, calculate_market_depth, get_quotes
from src.database.client import DatabaseClient
from datetime import datetime
from src.model import INVENTORY_BALANCE, INVENTORY_LIMIT
from loguru import logger

MEXC_TICK_SIZE = Decimal('0.00001')

lowest_possible_ask_price = Decimal('0')
highest_possible_bid_price = Decimal('0')

async def reset_orders(mexc_client: MexcClient):
    active_orders = mexc_client.get_active_orders()

    while True:
        await asyncio.sleep(30 * 60)
        logger.info("Resetting orders on MEXC...")
        cancellation = await mexc_client.cancel_all_orders(first_currency=CryptoCurrency.RMV, second_currency=CryptoCurrency.USDT)

        if cancellation is not None:
            active_orders.asks.clear()
            active_orders.bids.clear()

async def manage_orders(mexc_client: MexcClient, kucoin_client: KucoinClient, database_client: DatabaseClient):
    mexc_orderbook = mexc_client.get_orderbook()
    kucoin_orderbook = kucoin_client.get_orderbook()
    balance = mexc_client.get_balance()
    active_orders = mexc_client.get_active_orders()

    number_of_asks, number_of_bids = 5, 5

    ask_price, bid_price = get_quotes(mexc_client=mexc_client, kucoin_client=kucoin_client)

    if ask_price is None and bid_price is None or len(mexc_orderbook.asks) == 0 or len(mexc_orderbook.bids) == 0 or len(kucoin_orderbook.asks) == 0 or len(kucoin_orderbook.bids) == 0:
        return

    mid_price = (mexc_orderbook.asks[0].price + mexc_orderbook.bids[0].price) / 2
    upper_bound = mid_price * Decimal('1.02')
    lower_bound = mid_price * Decimal('0.98')

    for i in range(len(active_orders.asks) - 1, -1, -1):
        if (active_orders.asks[i].price <= ask_price - MEXC_TICK_SIZE) or (active_orders.asks[i].price >= ask_price + number_of_asks * MEXC_TICK_SIZE) or (active_orders.asks[i].price == ask_price and active_orders.asks[i].size > Decimal('5_000')) or (active_orders.asks[i].price > upper_bound and active_orders.asks[i].size > Decimal('20_000')):
            order_id = active_orders.asks[i].id
            cancellation = await mexc_client.cancel_order(first_currency=CryptoCurrency.RMV, second_currency=CryptoCurrency.USDT, order_id=order_id)

            if cancellation is not None:
                for j in range(len(active_orders.asks)):
                    if active_orders.asks[j].id == order_id:
                        del active_orders.asks[j]
                        break

    for i in range(len(active_orders.bids) - 1, -1, -1):
        if (active_orders.bids[i].price >= bid_price + MEXC_TICK_SIZE) or (active_orders.bids[i].price <= bid_price - number_of_bids * MEXC_TICK_SIZE) or (active_orders.bids[i].price == bid_price and active_orders.bids[i].size > Decimal('5_000')) or (active_orders.bids[i].price < lower_bound and active_orders.bids[i].size > Decimal('20_000')):
            order_id = active_orders.bids[i].id
            cancellation = await mexc_client.cancel_order(first_currency=CryptoCurrency.RMV, second_currency=CryptoCurrency.USDT, order_id=order_id)

            if cancellation is not None:
                for j in range(len(active_orders.bids)):
                    if active_orders.bids[j].id == order_id:
                        del active_orders.bids[j]
                        break

    max_size = Decimal('0')
    if len(active_orders.asks) < number_of_asks:
        max_size = balance['RMV']['free'] / Decimal(number_of_asks - len(active_orders.asks))

    while len(active_orders.asks) < number_of_asks:
        found = any(d.price == ask_price for d in active_orders.asks)

        if not found:
            size = Decimal(min(random.randint(2_000, 4_000), max_size))
            size = size.quantize(Decimal('1'), rounding=ROUND_DOWN)

            if max_size <= Decimal('400'):
                max_size = balance['RMV']['free']

            if size <= 0 or balance['RMV']['free'] <= Decimal('400'): # order value can't be less than 1 USDT
                logger.warning(f"To small balance: {balance['RMV']['free']} RMV")
                break

            order_id = await mexc_client.place_limit_order(first_currency=CryptoCurrency.RMV,second_currency=CryptoCurrency.USDT, side='sell',order_type='limit', size=size, price=ask_price)

            if order_id is not None:
                found = any(order_id == order.id for order in active_orders.asks)
                if not found:
                    active_orders.asks.append(OrderLevel(id=order_id, price=ask_price, size=size))
                    active_orders.asks.sort(key=lambda ask: ask.price)

                    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                    order = DatabaseOrder(pair='RMV-USDT', side='sell', price=ask_price, size=size, order_id=order_id, timestamp=timestamp)
                    await database_client.record_order(order=order, table_name="every_order_placed")
            else:
                break

        ask_price += MEXC_TICK_SIZE

    max_size_in_usdt = Decimal('0')
    if len(active_orders.bids) < number_of_bids:
        max_size_in_usdt = balance['USDT']['free'] / Decimal(number_of_bids - len(active_orders.bids))

    while len(active_orders.bids) < number_of_bids:
        found = any(d.price == bid_price for d in active_orders.bids)

        if not found:
            if max_size_in_usdt <= Decimal('1.1'):
                max_size_in_usdt = balance['USDT']['free']

            size = Decimal(min(random.randint(2_000, 4_000), max_size_in_usdt / bid_price))
            size = size.quantize(Decimal('1'), rounding=ROUND_DOWN)

            if size <= 0 or balance['USDT']['free'] <= Decimal('1.1'): # order value can't be less than 1 USDT
                logger.warning(f"To small balance: {balance['USDT']['free']} USDT")
                break

            order_id = await mexc_client.place_limit_order(first_currency=CryptoCurrency.RMV, second_currency=CryptoCurrency.USDT, side='buy',order_type='limit', size=size, price=bid_price)

            if order_id is not None:
                found = any(order_id == order.id for order in active_orders.bids)
                if not found:
                    active_orders.bids.append(OrderLevel(id=order_id, price=bid_price, size=size))
                    active_orders.bids.sort(key=lambda bid: bid.price, reverse=True)

                    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                    order = DatabaseOrder(pair='RMV-USDT', side='buy', price=bid_price, size=size, order_id=order_id, timestamp=timestamp)
                    await database_client.record_order(order=order, table_name="every_order_placed")
            else:
                break

        bid_price -= MEXC_TICK_SIZE


async def check_market_depth(mexc_client: MexcClient, database_client: DatabaseClient, percent: Decimal, expected_market_depth: Decimal):
    mexc_orderbook = mexc_client.get_orderbook()
    active_orders = mexc_client.get_active_orders()
    mexc_balance = mexc_client.get_balance()

    if len(mexc_orderbook.asks) == 0 or len(mexc_orderbook.bids) == 0 or 'USDT' not in mexc_balance or 'RMV' not in mexc_balance:
        return None

    lowest_ask = mexc_orderbook.asks[0].price
    highest_bid = mexc_orderbook.bids[0].price
    mid_price = (lowest_ask + highest_bid) / 2

    upper_bound = mid_price * (1 + percent / 100)
    lower_bound = mid_price * (1 - percent / 100)

    market_depth = calculate_market_depth(client=mexc_client, percent=percent)

    if len(active_orders.asks) == 0 or len(active_orders.bids) == 0:
        market_depth = 0

        for ask in active_orders.asks:
            market_depth += ask.size * ask.price
        for bid in active_orders.bids:
            market_depth += bid.size * bid.price

        logger.warning(f'market depth: {market_depth}')


    if market_depth < expected_market_depth * Decimal('0.98'):
        mexc_orderbook = mexc_client.get_orderbook()
        how_many_to_add = expected_market_depth - market_depth

        usdt_balance = mexc_balance['USDT']['free']
        rmv_balance = mexc_balance['RMV']['free']
        mid_price = (mexc_orderbook.asks[0].price + mexc_orderbook.bids[0].price) / 2
        rmv_value = mid_price * rmv_balance

        if len(active_orders.asks) == 0 or len(active_orders.bids) == 0:
            upper_bound = Decimal(1)
            lower_bound = Decimal(0)

        total_value = usdt_balance + rmv_value

        how_many_to_add_usdt = how_many_to_add * (usdt_balance / total_value)
        how_many_to_add_rmv = how_many_to_add * (rmv_value / total_value)

        ask_id = len(active_orders.asks) - 1
        stopper = 0

        while how_many_to_add_rmv > 1 and stopper < 100 and len(active_orders.asks) > 1:
            if mexc_balance['RMV']['free'] < 400:
                logger.warning('to small balance to add RMV volume')
                break

            if ask_id < 1:
                ask_id = len(active_orders.asks) - 1

            if 1 <= ask_id and active_orders.asks[ask_id].size < Decimal(290_000) and upper_bound >= active_orders.asks[ask_id].price:
                price = active_orders.asks[ask_id].price

                to_add = Decimal(min(random.randint(8_000, 10_000), mexc_balance['RMV']['free'] * Decimal('0.999')))
                size = to_add + active_orders.asks[ask_id].size
                size = size.quantize(Decimal('1'), rounding=ROUND_DOWN)

                order_id = active_orders.asks[ask_id].id
                cancellation = await mexc_client.cancel_order(first_currency=CryptoCurrency.RMV, second_currency=CryptoCurrency.USDT, order_id=order_id)

                if cancellation is not None:
                    for j in range(len(active_orders.asks)):
                        if active_orders.asks[j].id == order_id:
                            del active_orders.asks[j]
                            break

                    order_id = await mexc_client.place_limit_order(first_currency=CryptoCurrency.RMV, second_currency=CryptoCurrency.USDT, side='sell', order_type='limit', size=size, price=price)

                    if order_id is not None:
                        found = any(order_id == order.id for order in active_orders.asks)
                        if not found:
                            active_orders.asks.append(OrderLevel(id=order_id, price=price, size=size))
                            active_orders.asks.sort(key=lambda x: x.price)

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

        while how_many_to_add_usdt > 1 and stopper < 100 and len(active_orders.bids) > 1:
            if mexc_balance['USDT']['free'] < 1:
                logger.warning('to small balance to add usdt volume')
                break

            if bid_id < 1:
                bid_id = len(active_orders.bids) - 1

            if 1 <= bid_id and active_orders.bids[bid_id].size < Decimal(290_000) and lower_bound <= active_orders.bids[bid_id].price:
                price = active_orders.bids[bid_id].price

                to_add = Decimal(min(random.randint(8_000, 10_000), mexc_balance['USDT']['free'] / price * Decimal('0.999')))
                size = to_add + active_orders.bids[bid_id].size
                size = size.quantize(Decimal('1'), rounding=ROUND_DOWN)

                order_id = active_orders.bids[bid_id].id
                cancellation = await mexc_client.cancel_order(first_currency=CryptoCurrency.RMV, second_currency=CryptoCurrency.USDT, order_id=order_id)

                if cancellation is not None:
                    for j in range(len(active_orders.bids)):
                        if active_orders.bids[j].id == order_id:
                            del active_orders.bids[j]
                            break

                    order_id = await mexc_client.place_limit_order(first_currency=CryptoCurrency.RMV, second_currency=CryptoCurrency.USDT, side='buy', order_type='limit', size=size, price=price)

                    if order_id is not None:
                        found = any(order_id == order.id for order in active_orders.bids)
                        if not found:
                            active_orders.bids.append(OrderLevel(id=order_id, price=price, size=size))
                            active_orders.bids.sort(key=lambda x: x.price, reverse=True)

                            timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                            order = DatabaseOrder(pair='RMV-USDT', side='buy', price=price, size=size, order_id=order_id,timestamp=timestamp)
                            await database_client.record_order(order=order, table_name="every_order_placed")

                        how_many_to_add_usdt -= to_add * price
                    else:
                        logger.error(f'Failed to place limit order: price: {price}, size: {size}, balances: {mexc_balance}')

            bid_id -= 1
            stopper += 1

    return market_depth