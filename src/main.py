import asyncio
import os
from dotenv import load_dotenv
from decimal import Decimal, getcontext

from src import mexc
from src.mexc.client import MexcClient
from src.model import CryptoCurrency, OrderBook
from src.kucoin.client import KucoinClient
from src.market.tracking import update_active_orders, manage_orders, track_market_spread, track_market_depth
from src.market.calculations import calculate_market_depth, calculate_fair_price
from loguru import logger

load_dotenv()

getcontext().prec = 6

api_key_mexc = os.getenv("API_KEY_MEXC")
api_secret_mexc = os.getenv("API_SECRET_MEXC")

EXPECTED_MARKET_DEPTH = Decimal(1000)

order_lock = asyncio.Lock()

def on_filled_order(data):
    update_active_orders(data=data, kucoin_client=kucoin_client)


async def on_orderbook_change():
    async with order_lock:
        await manage_orders(mexc_client=mexc_client, kucoin_client=kucoin_client)
#     # print("orderbook has changed")
#     mexc_orderbook = mexc_client.get_orderbook()
#     kucoin_orderbook = kucoin_client.get_orderbook()
#     if len(mexc_orderbook.asks) == 0 or len(kucoin_orderbook.asks) == 0:
#         return

# async def afo():
#     while True:
#         async with order_lock:
#             await add_fair_orders(mexc_client=mexc_client, kucoin_client=kucoin_client)
#         await asyncio.sleep(0.5)

# async def tms():
#     while True:
#         await asyncio.sleep(10)
#         await track_market_spread(mexc_client=mexc_client)

async def tmd():
    while True:
        async with order_lock:
            await track_market_depth(mexc_client=mexc_client, kucoin_client=kucoin_client, percent=Decimal(2), expected_market_depth=EXPECTED_MARKET_DEPTH)
        await asyncio.sleep(0.5)


mexc_client = MexcClient(api_key=api_key_mexc, api_secret=api_secret_mexc, on_orderbook_change=on_orderbook_change, on_filled_order=on_filled_order)
kucoin_client = KucoinClient(on_orderbook_change=on_orderbook_change)


async def main():
    await mexc_client.cancel_all_orders()

    ready_event = asyncio.Event()
    asyncio.create_task(mexc_client.update_balance(ready_event=ready_event))
    await ready_event.wait()

    listen_key = await mexc_client.create_listen_key()
    asyncio.create_task(mexc_client.update_orderbook(first_currency=CryptoCurrency.RMV, second_currency=CryptoCurrency.USDT))
    asyncio.create_task(kucoin_client.update_orderbook(first_currency=CryptoCurrency.RMV, second_currency=CryptoCurrency.USDT))
    asyncio.create_task(mexc_client.track_active_orders(listen_key=listen_key))


    # asyncio.create_task(afo())
    asyncio.create_task(tmd())

    while True:
        await asyncio.sleep(10)
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

    # mexc_client.cancel_all_orders()

    # while True:
    #     await asyncio.sleep(1)
    #     print(mexc_client.get_balance()['USDT'])

    # await mexc_client._get_orderbook_snapshot(first_currency=CryptoCurrency.RMV, second_currency=CryptoCurrency.USDT)

if __name__ == '__main__':
    asyncio.run(main())

# add skewing, look at the balance, make event queue