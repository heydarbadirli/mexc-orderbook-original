import asyncio
import os
from dotenv import load_dotenv
from decimal import Decimal, getcontext

from src import mexc
from src.mexc.client import MexcClient
from src.model import CryptoCurrency, OrderBook
from src.kucoin.client import KucoinClient
from src.market.tracking import update_active_orders, add_fair_orders, track_market_spread, track_market_depth
from src.market.calculations import calculate_market_depth, calculate_fair_price
from loguru import logger

load_dotenv()

getcontext().prec = 6

api_key_mexc = os.getenv("API_KEY_MEXC")
api_secret_mexc = os.getenv("API_SECRET_MEXC")

EXPECTED_MARKET_DEPTH = Decimal(200)


async def on_orderbook_change():
    pass
#     # print("orderbook has changed")
#     mexc_orderbook = mexc_client.get_orderbook()
#     kucoin_orderbook = kucoin_client.get_orderbook()
#     if len(mexc_orderbook.asks) == 0 or len(kucoin_orderbook.asks) == 0:
#         return
async def afo():
    while True:
        await add_fair_orders(mexc_client=mexc_client, kucoin_client=kucoin_client)
        await asyncio.sleep(1)

# async def tms():
#     while True:
#         await asyncio.sleep(10)
#         await track_market_spread(mexc_client=mexc_client)

async def tmd():
    await asyncio.sleep(20)
    while True:
        await track_market_depth(mexc_client=mexc_client, percent=Decimal(2), expected_market_depth=EXPECTED_MARKET_DEPTH)
        await asyncio.sleep(1)


mexc_client = MexcClient(api_key=api_key_mexc, api_secret=api_secret_mexc, on_orderbook_change=on_orderbook_change, update_active_orders=update_active_orders)
kucoin_client = KucoinClient(on_orderbook_change=on_orderbook_change)


async def main():
    asyncio.create_task(mexc_client.update_orderbook(first_currency=CryptoCurrency.RMV, second_currency=CryptoCurrency.USDT))
    asyncio.create_task(mexc_client.update_balances())
    asyncio.create_task(kucoin_client.update_orderbook(first_currency=CryptoCurrency.RMV, second_currency=CryptoCurrency.USDT))
    asyncio.create_task(afo())
    asyncio.create_task(tmd())

    while True:
        await asyncio.sleep(60)
        logger.info(f'balance :{mexc_client.get_balance()}')
        logger.info(f"market depth: {calculate_market_depth(client=mexc_client, percent=Decimal('2'))}")
        logger.info(f"fair price: {calculate_fair_price(mexc_client=mexc_client, kucoin_client=kucoin_client, active_asks=[], active_bids=[], percent=Decimal('2'))}")
        logger.info(f'market spread: {await track_market_spread(mexc_client=mexc_client)}')

    mexc_client.cancel_all_orders()

    # while True:
    #     await asyncio.sleep(1)
    #     print(mexc_client.get_balance()['USDT'])

    # await mexc_client.get_active_orders()


if __name__ == '__main__':
    asyncio.run(main())

# add skewing, look at the balance, make event queue