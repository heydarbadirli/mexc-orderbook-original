import asyncio
import os
from dotenv import load_dotenv
from decimal import Decimal, getcontext
from datetime import datetime
from src.crypto.mexc.client import MexcClient
from src.model import CryptoCurrency, DatabaseMarketState, QueueEvent, EventType
from src.crypto.kucoin.client import KucoinClient
from src.crypto.market.tracking import manage_orders, check_market_depth, reset_orders, fix_price_if_too_large_inventory_imbalance
from src.crypto.market.calculations import calculate_market_depth, calculate_fair_price, calculate_market_spread
from loguru import logger
from src.database.client import DatabaseClient
import signal
import traceback

load_dotenv()

getcontext().prec = 18

api_key_mexc = os.getenv("API_KEY_MEXC")
api_secret_mexc = os.getenv("API_SECRET_MEXC")

api_key_kucoin = os.getenv("API_KEY_KUCOIN")
api_secret_kucoin = os.getenv("API_SECRET_KUCOIN")
api_passphrase_kucoin = os.getenv("API_PASSPHRASE_KUCOIN")

mysql_host = os.getenv("MYSQL_HOST")
mysql_user = os.getenv("MYSQL_USER")
mysql_password = os.getenv("MYSQL_PASSWORD")

EXPECTED_MARKET_DEPTH = Decimal(2500)

event_queue: asyncio.Queue[QueueEvent] = asyncio.Queue()

# there is event queue which is queue where functions puts event that are important for keeping orders with correct price and for keeping market depth
# there are two functions that that update orderbook: one updates kucoin_orderbook (kucoin_client.update_orderbook) and the other one updates mexc_orderbook (mexc_client.update_orderbook)
# when orderbook change they put event to queue
# there is function that tracks our active orders and when some order is filled it put event on the queue
# read_from_queue() is running all the time and in case of each type of event, invoke different function

async def add_to_event_queue(event: QueueEvent):
    await event_queue.put(event)

async def read_from_queue():
    while True:
        event = await event_queue.get()
        # logger.info(f'q size: {event_queue.qsize()}')

        if not event or event.type is None:
            logger.warning(f'Skipping invalid event: {event}')
            continue

        try:
            if event.type == EventType.KUCOIN_ORDERBOOK_UPDATE:
                await manage_orders(mexc_client=mexc_client, kucoin_client=kucoin_client, database_client=database_client)
            elif event.type == EventType.MEXC_ORDERBOOK_UPDATE:
                await manage_orders(mexc_client=mexc_client, kucoin_client=kucoin_client, database_client=database_client)
                await check_market_depth(mexc_client=mexc_client, database_client=database_client, percent=Decimal(2), expected_market_depth=EXPECTED_MARKET_DEPTH)
            elif event.type == EventType.FILLED_ORDER:
                ...
        except Exception as e:
            logger.error(f"error type: {type(e)}, details: {e}")
            logger.error(traceback.format_exc())

# handle exit cancels all our active orders when the program ends

def handle_exit(sig, frame):
    asyncio.get_event_loop().create_task(cancel_orders_and_exit())


async def cancel_orders_and_exit():
    logger.info("Cancelling all orders on MEXC...")
    try:
        await database_client.close()
        await mexc_client.cancel_all_orders(first_currency=CryptoCurrency.RMV, second_currency=CryptoCurrency.USDT)
    except Exception as e:
        logger.error(f"Error cancelling orders: {e}")
    asyncio.get_event_loop().stop()

signal.signal(signal.SIGINT, handle_exit)

database_client = DatabaseClient(host=mysql_host, user=mysql_user, password=mysql_password)
mexc_client = MexcClient(api_key=api_key_mexc, api_secret=api_secret_mexc, add_to_event_queue=add_to_event_queue, database_client=database_client)
kucoin_client = KucoinClient(api_key=api_key_kucoin, api_secret=api_secret_kucoin, api_passphrase=api_passphrase_kucoin, add_to_event_queue=add_to_event_queue, database_client=database_client)


async def main(): # all o this run concurrently
    await asyncio.sleep(30)

    await mexc_client.cancel_all_orders(first_currency=CryptoCurrency.RMV, second_currency=CryptoCurrency.USDT)
    await database_client.connect()

    listen_key = await mexc_client.create_listen_key()
    asyncio.create_task(mexc_client.extend_listen_key(listen_key=listen_key))
    asyncio.create_task(mexc_client.track_balance(listen_key=listen_key))
    asyncio.create_task(mexc_client.track_active_orders(listen_key=listen_key))

    asyncio.create_task(mexc_client.update_orderbook(first_currency=CryptoCurrency.RMV, second_currency=CryptoCurrency.USDT))

    asyncio.create_task(kucoin_client.update_orderbook(first_currency=CryptoCurrency.RMV, second_currency=CryptoCurrency.USDT))

    asyncio.create_task(read_from_queue())
    asyncio.create_task(reset_orders(mexc_client=mexc_client))

    asyncio.create_task(fix_price_if_too_large_inventory_imbalance(mexc_client=mexc_client, kucoin_client=kucoin_client))
    asyncio.create_task(mexc_client.reset_bought_and_sold_amounts())

    mexc_balance = mexc_client.get_balance()
    active_orders = mexc_client.get_active_orders()

    while True:
        await asyncio.sleep(10)
        logger.info('start')

        market_depth = calculate_market_depth(client=mexc_client, percent=Decimal('2'))
        fair_price = calculate_fair_price(mexc_client=mexc_client, kucoin_client=kucoin_client, active_asks=[], active_bids=[], percent=Decimal('2'))
        market_spread = calculate_market_spread(client=mexc_client)

        logger.info(f"market depth: {market_depth}")
        logger.info(f"fair price: {fair_price}")
        logger.info(f"market spread: {market_spread}")
        logger.info(f'len of active asks: {len(active_orders.asks)}')
        logger.info(f'asks: {active_orders.asks}')
        logger.info(f'len of active bids: {len(active_orders.bids)}')
        logger.info(f'bids: {active_orders.bids}')

        for i in range(1, len(active_orders.asks)):
            if active_orders.asks[i].price == active_orders.asks[i - 1].price:
                logger.error('Something is wrong')

        for i in range(1, len(active_orders.bids)):
            if active_orders.bids[i].price == active_orders.bids[i - 1].price:
                logger.error('Something is wrong')

        if fair_price is None:
            print()
            continue

        logger.info(f"usdt free balance: {mexc_balance['USDT']['free']}")
        logger.info(f"usdt locked balance: {mexc_balance['USDT']['locked']}")
        logger.info(f"usdt full balance: {mexc_balance['USDT']['free'] + mexc_balance['USDT']['locked']}")

        logger.info(f"rmv free balance {mexc_balance['RMV']['free']}, approximated usd value: {mexc_balance['RMV']['free'] * fair_price}")
        logger.info(f"rmv locked balance: {mexc_balance['RMV']['locked']}, approximate usd value: {mexc_balance['RMV']['locked'] * fair_price}")
        logger.info(f"rmv full balance: {mexc_balance['RMV']['free'] + mexc_balance['RMV']['locked']}, approximated usd value: {(mexc_balance['RMV']['free'] + mexc_balance['RMV']['locked']) * fair_price}")

        full_account_balance = mexc_balance['USDT']['free'] + mexc_balance['USDT']['locked'] + (mexc_balance['RMV']['free'] + mexc_balance['RMV']['locked']) * fair_price
        logger.info(f"full_account_balance: {full_account_balance}")

        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        market_state = DatabaseMarketState(market_depth=market_depth, fair_price=fair_price, market_spread=market_spread, usdt_balance=mexc_balance['USDT']['free'] + mexc_balance['USDT']['locked'], rmv_balance=mexc_balance['RMV']['free'] + mexc_balance['RMV']['locked'], rmv_value=mexc_balance['RMV']['free'] * fair_price + mexc_balance['RMV']['locked'] * fair_price, timestamp=timestamp)
        await database_client.record_market_state(market_state=market_state)

        logger.info('end\n')

if __name__ == '__main__':
    asyncio.run(main())