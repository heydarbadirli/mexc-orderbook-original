import websockets
import http.client
import json
from loguru import logger
from src.model import CryptoCurrency, OrderBook, OrderLevel, ExchangeClient
from decimal import Decimal
import asyncio


class KucoinClient(ExchangeClient):
    def __init__(self, on_orderbook_change=None):
        self.orderbook = OrderBook(asks=[], bids=[])
        self.on_orderbook_change = on_orderbook_change

    def get_orderbook(self):
        return self.orderbook


    @staticmethod
    async def _get_ws_url_public():
        connection = http.client.HTTPSConnection("api.kucoin.com")
        payload = ''
        headers = {}
        connection.request("POST", "/api/v1/bullet-public", payload, headers)
        response = connection.getresponse()
        data = response.read()
        parsed_data = json.loads(data)
        ws_url = parsed_data["data"]["instanceServers"][0]["endpoint"] + "?token=" + parsed_data["data"]["token"]

        return ws_url


    async def update_orderbook(self, first_currency: CryptoCurrency, second_currency: CryptoCurrency):
        symbol = first_currency.value + '-' + second_currency.value

        while True:
            try:
                ws_url = await self._get_ws_url_public()
                subscribe_message = {
                    "id": "sub-001",
                    "type": "subscribe",
                    "topic": f"/spotMarket/level2Depth50:{symbol}",
                    "response": True
                }

                async with websockets.connect(ws_url, ping_interval=20, ping_timeout=20) as ws:
                    await ws.send(json.dumps(subscribe_message))
                    logger.info(f"Subscribed to topic {first_currency.value + second_currency.value}, KUCOIN")

                    async for message in ws:
                        try:
                            # message = await ws.recv()
                            data = json.loads(message)

                            if data['type'] != "message":
                                continue

                            asks = [OrderLevel(price=Decimal(str(a[0])), size=Decimal(str(a[1]))) for a in data['data']['asks']]
                            bids = [OrderLevel(price=Decimal(str(a[0])), size=Decimal(str(a[1]))) for a in data['data']['bids']]

                            self.orderbook = OrderBook(asks=asks, bids=bids)
                            asyncio.create_task(self.on_orderbook_change())
                        except Exception as e:
                            logger.error(f'Exception: {e}')
            except Exception as e:
                logger.error(f"Error: {e}")
                await asyncio.sleep(5)