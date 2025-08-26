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
    def __init__(self, api_key: str, api_secret: str, add_to_event_queue=None):
        self.api_key = api_key
        self.api_secret = api_secret
        self.orderbook = OrderBook(asks=[], bids=[])
        self.balances = {}
        self.ws_base_url = "wss://wbs-api.mexc.com/ws"
        self.rest_base_url = "https://api.mexc.com"
        # self.on_orderbook_change = on_orderbook_change
        # self.on_filled_order = on_filled_order
        self.add_to_event_queue = add_to_event_queue


    def get_orderbook(self):
        return self.orderbook

    def get_balance(self):
        return self.balances

    def get_signature(self, query_string: str):
        return hmac.new(self.api_secret.encode('utf-8'), query_string.encode('utf-8'), hashlib.sha256).hexdigest()


    async def create_listen_key(self):
        url = self.rest_base_url + '/api/v3/userDataStream'

        timestamp = str(int(time.time() * 1000))
        params = {
            'timestamp': timestamp
        }

        query_string = urlencode(params)
        signature = self.get_signature(query_string=query_string)
        params['signature'] = signature

        headers = {
            "X-MEXC-APIKEY": self.api_key,
            "Content-Type": "application/json"
        }

        async with aiohttp.ClientSession() as session:
            async with session.post(url, headers=headers, params=params) as response:
                data = await response.json()
                return data['listenKey']


    async def extend_listen_key(self, listen_key):
        while True:
            await asyncio.sleep(30 * 60)
            url = self.rest_base_url + '/api/v3/userDataStream'

            timestamp = str(int(time.time() * 1000))
            params = {
                'timestamp': timestamp,
                'listenKey': listen_key
            }

            query_string = urlencode(params)
            signature = self.get_signature(query_string=query_string)
            params['signature'] = signature

            headers = {
                "X-MEXC-APIKEY": self.api_key,
                "Content-Type": "application/json"
            }

            async with aiohttp.ClientSession() as session:
                async with session.put(url, headers=headers, params=params) as response:
                    data = await response.json()
                    logger.info(f'Extended listenKey: {data}')


    async def track_active_orders(self, listen_key: str):
        url = f"wss://wbs.mexc.com/ws?listenKey={listen_key}"

        while True:
            try:
                async with websockets.connect(url, ping_interval=20, ping_timeout=20) as ws:
                    subscribe_message = {
                        "method": "SUBSCRIPTION",
                        "params": ["spot@private.orders"]
                    }

                    await ws.send(json.dumps(subscribe_message))

                    async for message in ws:
                        try:
                            data = json.loads(message)

                            if 'msg' in data and data['msg'] == "spot@private.orders":
                                logger.info("Subscribed to MEXC order tracking")
                            elif 'c' in data and data['c'] == 'spot@private.orders' and data['d']['status'] == 2 or data['d']['status'] == 3:
                                data = data['d']
                                logger.info(f"tracking orders mexc, data: {data}")
                                # await self.on_filled_order(data=data)
                                await self.add_to_event_queue(type="filled order", data=data)
                        except Exception as e:
                            logger.error(f"Error: {e}")

            except websockets.exceptions.ConnectionClosedOK as e:
                logger.info(f"WebSocket closed normally (1000 Bye): {e}. Reconnecting...")
                await asyncio.sleep(5)

            except Exception as e:
                logger.error(f"Error: {e}")
                await asyncio.sleep(5)


    # async def update_balances(self, listen_key: str):
    #     url = self.ws_base_url + f'?listenKey={listen_key}'
    #     print(url)
    #
    #     async with websockets.connect(url) as ws:
    #         subscribe_message = {
    #             "method": "SUBSCRIPTION",
    #             "params": ["spot@private.account.v3.api.pb"]
    #         }
    #
    #         await ws.send(json.dumps(subscribe_message))
    #         logger.info(f'Subscribed to mexc balance tracking')
    #         while True:
    #             try:
    #                 raw_data = await ws.recv()
    #                 data = json.loads(raw_data)
    #                 logger.info(f'Balanced has changed: {data}')
    #             except Exception as e:
    #                 logger.error(f"Error: {e}")


    async def update_orderbook(self, first_currency: CryptoCurrency, second_currency: CryptoCurrency):
        symbol = first_currency.value + second_currency.value
        while True:
            try:
                async with websockets.connect(self.ws_base_url, ping_interval=20, ping_timeout=20) as ws:
                    subscribe_message = {
                        "method": "SUBSCRIPTION",
                        "params": [f"spot@public.limit.depth.v3.api.pb@{symbol}@10"]
                    }

                    await ws.send(json.dumps(subscribe_message))
                    logger.info(f'Subscribed to topic, MEXC')

                    async for message in ws:
                        try:
                            if isinstance(message, str):
                                continue
                            # logger.info(f'orderbook update mexc')

                            result = PushDataV3ApiWrapper_pb2.PushDataV3ApiWrapper()
                            result.ParseFromString(message)

                            orderbook_dict = MessageToDict(result)
                            data = {'asks': orderbook_dict['publicLimitDepths']['asks'], 'bids': orderbook_dict['publicLimitDepths']['bids']}

                            asks = [OrderLevel(price=Decimal(str(ask['price'])), size=Decimal(str(ask['quantity']))) for ask in data['asks']]
                            bids = [OrderLevel(price=Decimal(str(bid['price'])), size=Decimal(str(bid['quantity']))) for bid in data['bids']]

                            if self.orderbook.asks == asks and self.orderbook.bids == bids:
                                continue

                            self.orderbook = OrderBook(asks=asks, bids=bids)
                            await self.add_to_event_queue(type="mexc orderbook update", data="")
                        except Exception as e:
                            logger.error(f"error: {e}")
            except Exception as e:
                logger.error(f'WebSocket connection error: {e}')
                await asyncio.sleep(5)


    async def place_limit_order(self, first_currency: CryptoCurrency, second_currency: CryptoCurrency, side: str, order_type: str, size: Decimal, price: Decimal):
        symbol = first_currency.value + second_currency.value
        url = self.rest_base_url + '/api/v3/order'

        timestamp = str(int(time.time() * 1000))
        params = {
            'type': order_type.upper(),
            'symbol': symbol,
            'side': side.upper(),
            'price': price,
            'quantity': size,
            'timestamp': timestamp
        }

        query_string = "&".join([f"{key}={params[key]}" for key in sorted(params.keys())])

        # signature = hmac.new(self.api_secret.encode('utf-8'), query_string.encode('utf-8'), hashlib.sha256).hexdigest()
        signature = self.get_signature(query_string=query_string)
        query_string += f'&signature={signature}'

        headers = {
            'X-MEXC-APIKEY': self.api_key,
            'Content-Type': 'application/json'
        }

        async with aiohttp.ClientSession() as session:
            async with session.post(url, headers=headers, params=query_string) as response:
                if response.status == 200:
                    data = await response.json()
                    return data['orderId']
                else:
                    return None


    async def cancel_order(self, first_currency: CryptoCurrency, second_currency: CryptoCurrency, order_id: str):
        symbol = first_currency.value + second_currency.value
        # url = 'https://api.mexc.com/api/v3/order'
        url = self.rest_base_url + '/api/v3/order'

        timestamp = str(int(time.time() * 1000))

        params = {
            'symbol': symbol.upper(),
            'orderId': order_id,
            'timestamp': timestamp,
            'api_key': self.api_key
        }

        query_string = urlencode(params)
        signature = hmac.new(self.api_secret.encode('utf-8'), query_string.encode('utf-8'),hashlib.sha256).hexdigest()
        params['signature'] = signature

        async with aiohttp.ClientSession() as session:
            async with session.delete(url, params=params) as response:
                data = await response.json()

                # if response.status == 200:
                #     logger.info(f"Order was cancelled on MEXC, id: {order_id}, data: {data}")

    async def cancel_all_orders(self):
        base_url = 'https://api.mexc.com'
        timestamp = round(time.time() * 1000)
        params = {
            'api_key': self.api_key,
            'symbol': 'RMVUSDT',
            'timestamp': timestamp
        }

        query_string = '&'.join([f'{key}={params[key]}' for key in sorted(params.keys())])
        signature = self.get_signature(query_string=query_string)
        # query_string += f'&signature={signature}'

        # async with aiohttp.ClientSession() as session:
        #     async with session.delete(url, params=params) as response:
        #         data = await response.json()

        cancel_endpoint = f'/api/v3/openOrders?api_key={self.api_key}' + '&symbol=RMVUSDT' + '&' + 'timestamp=' + str(timestamp) + '&' + 'signature=' + signature

        response = requests.delete(self.rest_base_url + cancel_endpoint)
        if response.status_code == 200:
            logger.info("Cancelled orders:", response.json())
        else:
            logger.error(response.status_code, response.text)


    async def update_balance(self):
        while True:
            try:
                timestamp = str(int(time.time() * 1000))
                query_string = f'api_key={self.api_key}&timestamp={timestamp}'
                signature = self.get_signature(query_string=query_string)

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

                        for token in data['balances']:
                            if token['asset'] == CryptoCurrency.RMV.value or token['asset'] == CryptoCurrency.USDT.value:
                                self.balances[token['asset']] = {'free': Decimal(token['free']), 'locked': Decimal(token['locked'])}
                await asyncio.sleep(10)
            except Exception as e:
                logger.error(f'error: {e}')