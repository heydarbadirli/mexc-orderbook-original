import websockets
import http.client
import json
from loguru import logger
from src.model import CryptoCurrency, OrderBook, OrderLevel, ExchangeClient
from decimal import Decimal


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
        ws_url = await self._get_ws_url_public()
        symbol = first_currency.value + '-' + second_currency.value

        subscribe_message = {
            "id": "sub-001",
            "type": "subscribe",
            "topic": f"/spotMarket/level2Depth50:{symbol}",
            "response": True
        }

        async with websockets.connect(ws_url) as ws:
            await ws.send(json.dumps(subscribe_message))
            logger.info(f"Subscribed to topic {first_currency.value + second_currency.value}, KUCOIN")

            while True:
                try:
                    message = await ws.recv()
                    data = json.loads(message)

                    if data['type'] != "message":
                        continue

                    asks = []
                    bids = []

                    for ask in data['data']['asks']:
                        asks.append(OrderLevel(price=Decimal(str(ask[0])), size=Decimal(str(ask[1]))))

                    for bid in data['data']['bids']:
                        bids.append(OrderLevel(price=Decimal(str(bid[0])), size=Decimal(str(bid[1]))))

                    self.orderbook = OrderBook(asks=asks, bids=bids)
                    await self.on_orderbook_change()

                except Exception as e:
                    logger.error(f"Error: {e}")