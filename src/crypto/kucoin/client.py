import websockets
import http.client
import json
from loguru import logger
from src.model import CryptoCurrency, OrderBook, OrderLevel, ExchangeClient, EventType, QueueEvent
from decimal import Decimal
import asyncio
import time
import base64
import hmac
import hashlib
from src.database.client import DatabaseClient
from datetime import datetime

class KucoinClient(ExchangeClient):
    def __init__(self, api_key: str, api_secret: str, api_passphrase: str, database_client: DatabaseClient, add_to_event_queue=None):
        super().__init__(add_to_event_queue=add_to_event_queue)
        self.api_key = api_key
        self.api_secret = api_secret
        self.api_passphrase = api_passphrase
        self.database_client = database_client

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
                            data = json.loads(message)

                            if data['type'] != "message":
                                continue

                            asks = [OrderLevel(price=Decimal(str(a[0])), size=Decimal(str(a[1])), id="") for a in data['data']['asks']]
                            bids = [OrderLevel(price=Decimal(str(a[0])), size=Decimal(str(a[1])), id="") for a in data['data']['bids']]

                            if self.orderbook.asks == asks and self.orderbook.bids == bids:
                                continue

                            self.orderbook = OrderBook(asks=asks, bids=bids)

                            timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                            await self.database_client.record_orderbook(table='kucoin_orderbook', exchange='kucoin', orderbook=self.orderbook, timestamp=timestamp)

                            event = QueueEvent(type=EventType.KUCOIN_ORDERBOOK_UPDATE, data=data)
                            await self.add_to_event_queue(event=event)
                        except Exception as e:
                            logger.error(f'Exception: {e}')
            except Exception as e:
                logger.error(f"Error: {e}")
                await asyncio.sleep(5)