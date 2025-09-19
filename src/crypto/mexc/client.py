from src.model import CryptoCurrency, OrderBook, OrderLevel, ExchangeClient, EventType, QueueEvent, DatabaseOrder
from src.database.client import DatabaseClient
import websockets
from src.crypto.mexc.websocket_proto import PushDataV3ApiWrapper_pb2
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
from datetime import datetime
import inspect

class MexcClient(ExchangeClient):
    def  __init__(self, api_key: str, api_secret: str, database_client: DatabaseClient, add_to_event_queue=None):
        self.api_key = api_key
        self.api_secret = api_secret
        self.orderbook = OrderBook(asks=[], bids=[])
        self.balance = {}
        self.ws_base_url = "wss://wbs-api.mexc.com/ws"
        self.rest_base_url = "https://api.mexc.com"
        self.add_to_event_queue = add_to_event_queue
        self.lock = asyncio.Lock()
        self.active_orders = OrderBook(asks=[], bids=[])
        self.database_client = database_client
        self.amount_sold = Decimal('0')
        self.amount_bought = Decimal('0')


    def get_orderbook(self):
        return self.orderbook


    def get_balance(self):
        return self.balance


    def get_active_orders(self):
        return self.active_orders


    def get_amount_bought(self):
        return self.amount_bought


    def get_amount_sold(self):
        return self.amount_sold


    def get_signature(self, query_string: str):
        return hmac.new(self.api_secret.encode('utf-8'), query_string.encode('utf-8'), hashlib.sha256).hexdigest()


    async def reset_bought_and_sold_amounts(self):
        while True:
            await asyncio.sleep(45 * 60)
            self.amount_sold = Decimal('0')
            self.amount_bought = Decimal('0')

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
        url = f'{self.ws_base_url}?listenKey={listen_key}'

        while True:
            try:
                async with websockets.connect(url, ping_interval=20, ping_timeout=20) as ws:
                    subscribe_message = {
                        "method": "SUBSCRIPTION",
                        "params": ["spot@private.orders.v3.api.pb"]
                    }

                    await ws.send(json.dumps(subscribe_message))
                    logger.info("Subscribed to MEXC order tracking")

                    async for message in ws:
                        try:
                            if isinstance(message, str):
                                continue

                            result = PushDataV3ApiWrapper_pb2.PushDataV3ApiWrapper()
                            result.ParseFromString(message)

                            data = MessageToDict(result)

                            if 'privateOrders' in data:
                                data = data['privateOrders']

                                if data['status'] == 1:
                                    side = 'buy' if data['tradeType'] == 1 else 'sell'
                                    price = Decimal(str(data['price']))
                                    size = Decimal(str(data['quantity']))
                                    order_id = data['id']

                                    # if side == 'buy':
                                    #     found = any(d.id == order_id for d in self.active_orders.bids)
                                    # else:
                                    #     found = any(d.id == order_id for d in self.active_orders.asks)
                                    #
                                    # if not found:
                                    #     logger.error(f"NOT FOUND order in active orders: {data}")

                                    # if side == 'buy':
                                    #     self.active_orders.bids.append(OrderLevel(id=order_id, price=price, size=size))
                                    #     self.active_orders.bids.sort(key=lambda x: x.price, reverse=True)
                                    # else:
                                    #     self.active_orders.asks.append(OrderLevel(id=order_id, price=price, size=size))
                                    #     self.active_orders.asks.sort(key=lambda x: x.price)
                                elif data['status'] == 2 or data['status'] == 3:
                                    side = 'buy' if data['tradeType'] == 1 else 'sell'
                                    order_id = data['id']
                                    price = Decimal(str(data['price']))
                                    remain_size = Decimal(str(data['remainQuantity']))
                                    trade_size = Decimal(str(data['cumulativeQuantity']))

                                    if side == 'buy':
                                        self.amount_bought += trade_size
                                    else:
                                        self.amount_sold += remain_size

                                    orders = self.active_orders.bids if side == 'buy' else self.active_orders.asks

                                    for i in range(len(orders) - 1, -1, -1):
                                        if orders[i].id == order_id:
                                            if data['status'] == 2:
                                                del orders[i]
                                            else:
                                                orders[i].size = remain_size

                                    logger.info(f"tracking orders mexc, data: {data}")
                                    event = QueueEvent(type=EventType.FILLED_ORDER, data=data)

                                    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                                    order_id = data['id']

                                    order = DatabaseOrder(pair='RMV-USDT', side=side, price=price, size=trade_size, timestamp=timestamp, order_id=order_id)
                                    await self.database_client.record_order(order=order, table_name="orders")

                                    await self.add_to_event_queue(event=event)
                                # elif data['status'] == 4 or data['status'] == 5:
                                #     side = 'buy' if data['tradeType'] == 1 else 'sell'
                                #     order_id = data['id']
                                #
                                #     if side == 'buy':
                                #         found = any(d.id == order_id for d in self.active_orders.bids)
                                #     else:
                                #         found = any(d.id == order_id for d in self.active_orders.asks)
                                #
                                #     if found:
                                #         logger.error(f"FOUND order in active orders, THIS ORDER SHOULD NOT EXISTS: {data}")

                                    # if side == 'buy':
                                    #     for i in range(len(self.active_orders.bids) - 1, -1, -1):
                                    #         if self.active_orders.bids[i].id == order_id:
                                    #             del self.active_orders.bids[i]
                                    #             break
                                    # else:
                                    #     for i in range(len(self.active_orders.asks) - 1, -1, -1):
                                    #         if self.active_orders.asks[i].id == order_id:
                                    #             del self.active_orders.asks[i]
                                    #             break

                                timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                                await self.database_client.record_orderbook(table='our_orders', exchange='mexc', orderbook=self.active_orders, timestamp=timestamp)

                        except Exception as e:
                            logger.error(f"Error: {e}")

            except websockets.exceptions.ConnectionClosedOK as e:
                logger.info(f"WebSocket closed normally (1000 Bye): {e}. Reconnecting...")
                await asyncio.sleep(5)

            except Exception as e:
                logger.error(f"Error: {e}")
                await asyncio.sleep(5)


    async def get_balance_snapshot(self):
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
                            self.balance[token['asset']] = {'free': Decimal(token['free']), 'locked': Decimal(token['locked'])}
        except Exception as e:
            logger.error(f'error: {e}')


    async def track_balance(self, listen_key: str):
        url = f'{self.ws_base_url}?listenKey={listen_key}'

        while True:
            try:
                async with websockets.connect(url, ping_interval=20, ping_timeout=20) as ws:
                    subscribe_message = {
                        "method": "SUBSCRIPTION",
                        "params": ["spot@private.account.v3.api.pb"]
                    }

                    await ws.send(json.dumps(subscribe_message))

                    await self.get_balance_snapshot()
                    logger.info('Fetched balance snapshot')

                    async for message in ws:
                        try:
                            if isinstance(message, str):
                                continue
                            result = PushDataV3ApiWrapper_pb2.PushDataV3ApiWrapper()
                            result.ParseFromString(message)

                            data = MessageToDict(result)

                            if 'privateAccount' in data:
                                token = data['privateAccount']['vcoinName']

                                self.balance[token] = {'free': Decimal(str(data['privateAccount']['balanceAmount'])), 'locked': Decimal(str(data['privateAccount']['frozenAmount']))}


                        except Exception as e:
                            logger.error(f"Error: {e}")
            except Exception as e:
                logger.error(f"Websocket connection error: {e}")
                await asyncio.sleep(5)


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

                            result = PushDataV3ApiWrapper_pb2.PushDataV3ApiWrapper()
                            result.ParseFromString(message)

                            orderbook_dict = MessageToDict(result)
                            data = {'asks': orderbook_dict['publicLimitDepths']['asks'], 'bids': orderbook_dict['publicLimitDepths']['bids']}

                            asks = [OrderLevel(price=Decimal(str(ask['price'])), size=Decimal(str(ask['quantity'])), id="") for ask in data['asks']]
                            bids = [OrderLevel(price=Decimal(str(bid['price'])), size=Decimal(str(bid['quantity'])), id="") for bid in data['bids']]

                            if self.orderbook.asks == asks and self.orderbook.bids == bids:
                                continue

                            self.orderbook = OrderBook(asks=asks, bids=bids)

                            timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                            await self.database_client.record_orderbook(table='mexc_orderbook', exchange='mexc', orderbook=self.orderbook, timestamp=timestamp)

                            event = QueueEvent(type=EventType.MEXC_ORDERBOOK_UPDATE, data=data)
                            await self.add_to_event_queue(event=event)
                        except Exception as e:
                            logger.error(f"error: {e}")
            except Exception as e:
                logger.error(f'WebSocket connection error: {e}')
                await asyncio.sleep(5)


    async def place_limit_order(self, first_currency: CryptoCurrency, second_currency: CryptoCurrency, side: str, order_type: str, size: Decimal, price: Decimal):
        async with self.lock:
            symbol = first_currency.value + second_currency.value
            url = self.rest_base_url + '/api/v3/order'

            timestamp = str(int(time.time() * 1000))
            params = {
                'type': order_type.upper(),
                'symbol': symbol,
                'side': side.upper(),
                'price': str(price),
                'quantity': str(size),
                'timestamp': timestamp
            }

            query_string = "&".join([f"{key}={params[key]}" for key in sorted(params.keys())])

            signature = self.get_signature(query_string=query_string)
            query_string += f'&signature={signature}'

            headers = {
                'X-MEXC-APIKEY': self.api_key,
                'Content-Type': 'application/json'
            }

            async with aiohttp.ClientSession() as session:
                async with session.post(url, headers=headers, params=query_string) as response:
                    text = await response.text()
                    if response.status == 200:
                        data = await response.json()
                        # logger.info(f"Successfully placed order: {data}")
                        # try:
                        #     caller_frame = inspect.stack()[1]
                        #     caller_file = caller_frame.filename
                        #     caller_line = caller_frame.lineno
                        #     caller_func = caller_frame.function
                        #     logger.info(f"Called from {caller_func} in {caller_file} at line {caller_line}")
                        # except Exception as e:
                        #     logger.error(f"Error: {e}")
                        return data['orderId']
                    else:
                        logger.error(f'Order failed: {text}, price: {price}, size: {size}, side: {side},  balances: {self.balance}')
                        try:
                            caller_frame = inspect.stack()[1]
                            caller_file = caller_frame.filename
                            caller_line = caller_frame.lineno
                            caller_func = caller_frame.function
                            logger.info(f"Called from {caller_func} in {caller_file} at line {caller_line}")
                        except Exception as e:
                            logger.error(f"Error: {e}")
                        return None


    async def cancel_order(self, first_currency: CryptoCurrency, second_currency: CryptoCurrency, order_id: str):
        symbol = first_currency.value + second_currency.value
        url = self.rest_base_url + '/api/v3/order'

        timestamp = str(int(time.time() * 1000))

        params = {
            'symbol': symbol.upper(),
            'orderId': order_id,
            'timestamp': timestamp,
            'recvWindow': 60000,
            'api_key': self.api_key
        }

        query_string = urlencode(params)
        signature = hmac.new(self.api_secret.encode('utf-8'), query_string.encode('utf-8'),hashlib.sha256).hexdigest()
        params['signature'] = signature

        async with aiohttp.ClientSession() as session:
            async with session.delete(url, params=params) as response:
                data = await response.json()

                if response.status == 200:
                    # logger.info(f'Successfully canceled order: {data}')
                    # try:
                    #     caller_frame = inspect.stack()[1]
                    #     caller_file = caller_frame.filename
                    #     caller_line = caller_frame.lineno
                    #     caller_func = caller_frame.function
                    #     logger.info(f"Called from {caller_func} in {caller_file} at line {caller_line}")
                    # except Exception as e:
                    #     logger.error(f"Error: {e}")
                    return data
                else:
                    logger.error(f'Order cancellation failed: {data}, order_id: {order_id}')
                    try:
                        caller_frame = inspect.stack()[1]
                        caller_file = caller_frame.filename
                        caller_line = caller_frame.lineno
                        caller_func = caller_frame.function
                        print(f"Called from {caller_func} in {caller_file} at line {caller_line}")
                    except Exception as e:
                        logger.error(f"Error: {e}")
                    return None


    async def cancel_all_orders(self, first_currency: CryptoCurrency, second_currency: CryptoCurrency):
        timestamp = round(time.time() * 1000)
        symbol = first_currency.value + second_currency.value

        params = {
            'api_key': self.api_key,
            'symbol': symbol.upper(),
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
            logger.info(f"Cancelled orders: {response.json()}")
            return response
        else:
            logger.error(f'{response.status_code}, {response.text}')
            return None
