import asyncio
import os
from dotenv import load_dotenv
from decimal import Decimal, getcontext
from datetime import datetime
from src.mexc.client import MexcClient
from src.model import CryptoCurrency, OrderBook, DatabaseMarketState, DatabaseOrder
from src.kucoin.client import KucoinClient
from src.market.tracking import update_active_orders, manage_orders, track_market_spread, track_market_depth
from src.market.calculations import calculate_market_depth, calculate_fair_price
from loguru import logger
from src.database.client import DatabaseClient

load_dotenv()

getcontext().prec = 6

api_key_mexc = os.getenv("API_KEY_MEXC")
api_secret_mexc = os.getenv("API_SECRET_MEXC")

mysql_host = os.getenv("MYSQL_HOST")
mysql_user = os.getenv("MYSQL_USER")
mysql_password = os.getenv("MYSQL_PASSWORD")

EXPECTED_MARKET_DEPTH = Decimal(1000)

order_lock = asyncio.Lock()

async def on_filled_order(data):
    await update_active_orders(data=data, kucoin_client=kucoin_client, database_client=database_client)


async def on_orderbook_change():
    async with order_lock:
        await manage_orders(mexc_client=mexc_client, kucoin_client=kucoin_client)

async def tmd(): # track market depth
    while True:
        async with order_lock:
            await track_market_depth(mexc_client=mexc_client, kucoin_client=kucoin_client, percent=Decimal(2), expected_market_depth=EXPECTED_MARKET_DEPTH)
        await asyncio.sleep(0.5)

# tmd function is running all the time and checking if market depth is ok, if it is not then it add size to our active orders

# there are two update_orderbook functions, one in mexc_client and one in kucoin_client
# update_orderbook function is running all the time and if something change, then it invoke on_orderbook_change function which invokes manage_orders function which add new orders, and check
# if we should cancel some of our active orders

# track_active_orders function is running all the time and if some of our orders is filled (can be partially filled) it invokes on_filled_order function which add this order to database
# and remove it from our list (there are two lists in tracking.py, active_bids and active_asks)

# tmd function and manage_orders function can't work in the same time because they change the same list (active_bids or active_asks) and that often cause conflicts
# other functions can run concurrently


mexc_client = MexcClient(api_key=api_key_mexc, api_secret=api_secret_mexc, on_orderbook_change=on_orderbook_change, on_filled_order=on_filled_order)
kucoin_client = KucoinClient(on_orderbook_change=on_orderbook_change)
database_client = DatabaseClient(host=mysql_host, user=mysql_user, password=mysql_password)


async def main():
    await mexc_client.cancel_all_orders()
    await database_client.connect()

    asyncio.create_task(mexc_client.update_balance())

    listen_key = await mexc_client.create_listen_key()
    asyncio.create_task(mexc_client.update_orderbook(first_currency=CryptoCurrency.RMV, second_currency=CryptoCurrency.USDT))
    asyncio.create_task(kucoin_client.update_orderbook(first_currency=CryptoCurrency.RMV, second_currency=CryptoCurrency.USDT))
    asyncio.create_task(mexc_client.track_active_orders(listen_key=listen_key))

    asyncio.create_task(tmd())

    #######################################################################################################################################################

    while True:
        await asyncio.sleep(60)
        balances = mexc_client.get_balance()

        market_depth = calculate_market_depth(client=mexc_client, percent=Decimal('2'))
        fair_price = calculate_fair_price(mexc_client=mexc_client, kucoin_client=kucoin_client, active_asks=[], active_bids=[], percent=Decimal('2'))
        market_spread = await track_market_spread(mexc_client=mexc_client)

        logger.info(f"market depth: {market_depth}")
        logger.info(f"fair price: {fair_price}")
        logger.info(f"market spread: {market_spread}")
        if fair_price is None:
            print()
            continue
        logger.info(f"usdt free balance: {balances['USDT']['free']}")
        logger.info(f"rmv free balance {balances['RMV']['free']}, approximated usd value: {balances['RMV']['free'] * fair_price}")
        logger.info(f"usdt locked balance: {balances['USDT']['locked']}")
        logger.info(f"rmv locked balance: {balances['RMV']['locked']} RMV, approximate usd value: {balances['RMV']['locked'] * fair_price}")
        full_account_balance = balances['USDT']['free'] + balances['USDT']['locked'] + balances['RMV']['free'] * fair_price + balances['RMV']['locked'] * fair_price
        logger.info(f"full_account_balance: {full_account_balance}\n")
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        market_state = DatabaseMarketState(market_depth=market_depth, fair_price=fair_price, market_spread=market_spread, usdt_balance=balances['USDT']['free'] + balances['USDT']['locked'], rmv_balance=balances['RMV']['free'] + balances['RMV']['locked'], rmv_value=balances['RMV']['free'] * fair_price + balances['RMV']['locked'] * fair_price, timestamp=timestamp)
        await database_client.record_market_state(market_state=market_state)


if __name__ == '__main__':
    asyncio.run(main())