from src.model import CryptoCurrency, OrderBook, OrderLevel, ExchangeClient
import websockets
from src.mexc.websocket_proto import PushDataV3ApiWrapper_pb2
from google.protobuf.json_format import MessageToDict
import json
from loguru import logger
from decimal import Decimal
import time
import hmac
import hashlib
import requests
import aiohttp
import asyncio
from urllib.parse import urlencode


class MexcClient(ExchangeClient):
    def __init__(self, api_key: str, api_secret: str, on_orderbook_change=None, update_active_orders=None):
        self.api_key = api_key
        self.api_secret = api_secret
        self.ws_url = "wss://wbs-api.mexc.com/ws"
        self.orderbook = OrderBook(asks=[], bids=[])
        self.balances = {}
        self.on_orderbook_change=on_orderbook_change
        self.update_active_orders = update_active_orders


    def get_orderbook(self):
        return self.orderbook


    def get_balance(self):
        return self.balances


    async def update_orderbook(self, first_currency: CryptoCurrency, second_currency: CryptoCurrency):
        symbol = first_currency.value + second_currency.value

        async with websockets.connect(self.ws_url) as ws:
            subscribe_message = {
                "method": "SUBSCRIPTION",
                "params": [f"spot@public.limit.depth.v3.api.pb@{symbol}@10"]
            }

            await ws.send(json.dumps(subscribe_message))
            logger.info(f'Subscribed to topic, MEXC')

            while True:
                try:
                    message = await ws.recv()

                    if isinstance(message, str):
                        continue

                    result = PushDataV3ApiWrapper_pb2.PushDataV3ApiWrapper()
                    result.ParseFromString(message)

                    orderbook_dict = MessageToDict(result)
                    data = {'asks': orderbook_dict['publicLimitDepths']['asks'], 'bids': orderbook_dict['publicLimitDepths']['bids']}

                    asks, bids = [], []

                    for ask in data['asks']:
                        asks.append(OrderLevel(price=Decimal(str(ask['price'])), size=Decimal(str(ask['quantity']))))
                    for bid in data['bids']:
                        bids.append(OrderLevel(price=Decimal(str(bid['price'])), size=Decimal(str(bid['quantity']))))

                    self.orderbook = OrderBook(asks=asks, bids=bids)
                    await self.on_orderbook_change()
                except Exception as e:
                    ...
                    # logger.error(f"error: {e}")


    async def place_limit_order(self, first_currency: CryptoCurrency, second_currency: CryptoCurrency, side: str, order_type: str, size: Decimal, price: Decimal):
        base_url = "https://api.mexc.com"
        symbol = first_currency.value + second_currency.value
        timestamp = str(int(time.time() * 1000))

        params = {
            'api_key': self.api_key,
            'type': order_type.upper(),
            'symbol': symbol,
            'side': side.upper(),
            'price': price,
            'quantity': size,
            'timestamp': timestamp
        }

        data = '&'.join([f'{key}={params[key]}' for key in sorted(params.keys())])
        signature = hmac.new(self.api_secret.encode(), data.encode(), hashlib.sha256).hexdigest()
        params['signature'] = signature
        trade_endpoint = f"/api/v3/order?api_key={params['api_key']}&price={str(params['price'])}&quantity={str(params['quantity'])}&side={params['side']}&symbol={params['symbol']}&timestamp={str(params['timestamp'])}&type={params['type']}&signature={signature}"

        response = requests.post(base_url + trade_endpoint)
        data = response.json()
        # logger.info(f"MEXC response status code: {response.status_code}, data: {data}")

        return data['orderId']


    async def update_balances(self):
        while True:
            try:
                timestamp = str(int(time.time() * 1000))
                query_string = f'api_key={self.api_key}&timestamp={timestamp}'
                signature = hmac.new(self.api_secret.encode('utf-8'), query_string.encode('utf-8'),
                                     hashlib.sha256).hexdigest().upper()

                url = 'https://api.mexc.com/api/v3/account'
                params = {
                    'api_key': self.api_key,
                    'timestamp': timestamp,
                    'signature': signature
                }

                headers = {
                    'X-MEXC-APIKEY': self.api_key,
                    'Content-Type': 'application/json'
                }

                async with aiohttp.ClientSession() as session:
                    async with session.get(url, headers=headers, params=params) as response:
                        data = await response.json()
                        # print(f'mexc {data}')

                        for token in data['balances']:
                            if token['asset'] == CryptoCurrency.RMV.value or token['asset'] == CryptoCurrency.USDT.value:
                                self.balances[token['asset']] = token['free']

                await asyncio.sleep(1)
            except Exception as e:
                logger.error(f'error: {e}')


    async def cancel_order(self, first_currency: CryptoCurrency, second_currency: CryptoCurrency, order_id: str):
        url = 'https://api.mexc.com/api/v3/order'
        symbol = first_currency.value + second_currency.value

        timestamp = str(int(time.time() * 1000))

        params = {
            'symbol': symbol.upper(),
            'orderId': order_id,
            'timestamp': timestamp,
            'api_key': self.api_key
        }

        query_string = urlencode(params)
        signature = hmac.new(self.api_secret.encode('utf-8'), query_string.encode('utf-8'), hashlib.sha256).hexdigest()
        params['signature'] = signature


        async with aiohttp.ClientSession() as session:
            async with session.delete(url, params=params) as response:
                data = await response.json()

                # if response.status == 200:
                #     logger.info(f"Order was cancelled on MEXC, id: {order_id}, data: {data}")


    async def _create_listen_key(self):
        url = 'https://api.mexc.com/api/v3/userDataStream'

        timestamp = str(int(time.time() * 1000))
        params = {
            'timestamp': timestamp
        }

        query_string = urlencode(params)
        signature = hmac.new(self.api_secret.encode('utf-8'), query_string.encode('utf-8'), hashlib.sha256).hexdigest()

        params['signature'] = signature

        headers = {
            "X-MEXC-APIKEY": self.api_key,
            "Content-Type": "application/json"
        }

        async with aiohttp.ClientSession() as session:
            async with session.post(url, headers=headers, params=params) as response:
                data = await response.json()
                return data['listenKey']


    async def track_active_orders(self):
        listen_key = await self._create_listen_key()
        url = f"wss://wbs.mexc.com/ws?listenKey={listen_key}"

        async with websockets.connect(url) as ws:
            subscribe_message = {
                "method": "SUBSCRIPTION",
                "params": ["spot@private.orders"]
            }

            await ws.send(json.dumps(subscribe_message))
            logger.info(f'Subscribed to mexc order tracking')
            while True:
                try:
                    raw_data = await ws.recv()
                    data = json.loads(raw_data)

                    if 'msg' in data and data['msg'] == "spot@private.orders":
                        logger.info("Subscribed to MEXC order tracking")
                    elif 'c' in data and data['c'] == 'spot@private.orders' and data['d']['status'] == 2:
                        data = data['d']
                        side = 'buy' if data['tradeType'] == 1 else 'sell'
                        logger.info(f"tracking orders mexc, data: {data}")
                        self.update_active_orders(side)
                    elif 'c' in data and data['c'] == 'spot@private.orders' and data['d']['status'] == 3:
                        data = data['d']
                        logger.info(f"tracking orders mexc, data: {data}")
                except Exception as e:
                    logger.error(f"Error {e}")


    def cancel_all_orders(self):
        timestamp = round(time.time() * 1000)
        base_url = 'https://api.mexc.com'
        params = {
            'api_key': self.api_key,
            'symbol': 'RMVUSDT',
            'timestamp': timestamp
        }
        data = '&'.join([f'{key}={params[key]}' for key in sorted(params.keys())])
        signature = hmac.new(self.api_secret.encode(), data.encode(), hashlib.sha256).hexdigest()

        cancel_endpoint = f'/api/v3/openOrders?api_key={self.api_key}' + '&symbol=RMVUSDT' + '&' + 'timestamp=' + str(timestamp) + '&' + 'signature=' + signature

        response = requests.delete(base_url + cancel_endpoint)
        if response.status_code == 200:
            logger.info("Cancelled orders:", response.json())
        else:
            logger.error(response.status_code, response.text)