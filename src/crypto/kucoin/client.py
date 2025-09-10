import websockets
import http.client
import json
from loguru import logger
from src.model import CryptoCurrency, OrderBook, OrderLevel, ExchangeClient, EventType, QueueEvent
from decimal import Decimal
import asyncio
import aiohttp
import uuid
import time
import base64
import hmac
import hashlib

class KucoinClient(ExchangeClient):
    def __init__(self, api_key: str, api_secret: str, api_passphrase: str, add_to_event_queue=None):
        self.orderbook = OrderBook(asks=[], bids=[])
        self.balances = {}
        self.add_to_event_queue = add_to_event_queue
        self.api_key = api_key
        self.api_secret = api_secret
        self.api_passphrase = api_passphrase


    def get_orderbook(self):
        return self.orderbook


    def get_balance(self):
        return self.balances


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
                            # logger.info('orderbook update kucoin')

                            if data['type'] != "message":
                                continue

                            asks = [OrderLevel(price=Decimal(str(a[0])), size=Decimal(str(a[1])), id="") for a in data['data']['asks']]
                            bids = [OrderLevel(price=Decimal(str(a[0])), size=Decimal(str(a[1])), id="") for a in data['data']['bids']]

                            if self.orderbook.asks == asks and self.orderbook.bids == bids:
                                continue

                            self.orderbook = OrderBook(asks=asks, bids=bids)
                            event = QueueEvent(type=EventType.KUCOIN_ORDERBOOK_UPDATE, data=data)
                            await self.add_to_event_queue(event=event)
                        except Exception as e:
                            logger.error(f'Exception: {e}')
            except Exception as e:
                logger.error(f"Error: {e}")
                await asyncio.sleep(5)


    def _get_headers(self, method, endpoint, body=''):
        now = str(int(time.time() * 1000))
        str_to_sign = now + method + endpoint + body

        signature = base64.b64encode(
            hmac.new(self.api_secret.encode('utf-8'), str_to_sign.encode('utf-8'), hashlib.sha256).digest()
        ).decode()

        passphrase = base64.b64encode(
            hmac.new(self.api_secret.encode('utf-8'), self.api_passphrase.encode('utf-8'), hashlib.sha256).digest()
        ).decode()

        headers = {
            "KC-API-KEY": self.api_key,
            "KC-API-SIGN": signature,
            "KC-API-TIMESTAMP": now,
            "KC-API-PASSPHRASE": passphrase,
            "KC-API-KEY-VERSION": "2",
            "Content-Type": "application/json"
        }
        return headers


    # async def update_balance(self):
    #     while True:
    #         endpoint = "/api/v1/accounts"
    #         method = "GET"
    #         body = ''
    #
    #         headers = self._get_headers(method=method, endpoint=endpoint, body=body)
    #         url = "https://api.kucoin.com/api/v1/accounts"
    #
    #         async with aiohttp.ClientSession() as session:
    #             async with session.get(url, headers=headers) as response:
    #                 data = await response.json()
    #
    #                 for token in data['data']:
    #                     if token['currency'] == CryptoCurrency.RMV.value or token['currency'] == CryptoCurrency.USDT.value:
    #                         self.balances[token['currency']] = Decimal(token['balance'])
    #         await asyncio.sleep(10)
    #
    #
    # async def place_limit_order(self, first_currency: CryptoCurrency, second_currency: CryptoCurrency, side: str, order_type: str, size: Decimal, price: Decimal):
    #     symbol = first_currency.value + '-' + second_currency.value
    #     url = "https://api.kucoin.com"
    #     endpoint = "/api/v1/hf/orders"
    #     method = "POST"
    #
    #     order = {
    #         "type": order_type,
    #         "symbol": symbol,
    #         "side": side,
    #         "price": str(price),
    #         "size": str(size),
    #         "clientOid": str(uuid.uuid4()),
    #         "remark": "order remarks"
    #     }
    #
    #     body = json.dumps(order)
    #     headers = self._get_headers(method=method, endpoint=endpoint, body=body)
    #
    #     async with aiohttp.ClientSession() as session:
    #         async with session.post(url + endpoint, headers=headers, data=body) as response:
    #             text = await response.json()
    #             status = response.status
    #             logger.info(f"KuCoin placing order response: {status}, data: {text}")
    #
    #             return text