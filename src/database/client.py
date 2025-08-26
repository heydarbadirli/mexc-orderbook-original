from loguru import logger
from src.model import DatabaseOrder, DatabaseMarketState, OrderBook
import aiomysql

class DatabaseClient:
    def __init__(self, host, user, password):
        self.host = host
        self.user = user
        self.password = password
        self.db = 'default'
        self.connection = None
        self.pool = None

    async def connect(self):
        self.pool = await aiomysql.create_pool(
            host=self.host,
            user=self.user,
            password=self.password,
            port=8888,
            db=self.db,
            autocommit=True,
            maxsize=10
        )

        logger.info("Successfully connected to MySql database")

        async with self.pool.acquire() as connection:
            async with connection.cursor() as cursor:
                await cursor.execute("""
                    CREATE TABLE IF NOT EXISTS orders (
                        id INT AUTO_INCREMENT PRIMARY KEY,
                        pair VARCHAR(50),
                        side VARCHAR(50),
                        quantity VARCHAR(50),
                        price VARCHAR(50),
                        timestamp DATETIME
                    )
                """)

        async with self.pool.acquire() as connection:
            async with connection.cursor() as cursor:
                await cursor.execute("""
                    CREATE TABLE IF NOT EXISTS market_states (
                        id INT AUTO_INCREMENT PRIMARY KEY,
                        market_depth VARCHAR(50),
                        fair_price VARCHAR(50),
                        market_spread VARCHAR(50),
                        usdt_balance VARCHAR(50),
                        rmv_balance VARCHAR(50),
                        rmv_value VARCHAR(50),
                        timestamp DATETIME
                    )
                """)


        for table_name in ['kucoin_orderbook', 'mexc_orderbook']:
            async with self.pool.acquire() as connection:
                async with connection.cursor() as cursor:
                    await cursor.execute(f"""
                        CREATE TABLE IF NOT EXISTS {table_name} (
                            id INT AUTO_INCREMENT PRIMARY KEY,
                            exchange VARCHAR(50),
                            symbol VARCHAR(50),
                            timestamp DATETIME,
                            
                            bid1_price VARCHAR(50), bid1_size VARCHAR(50),
                            bid2_price VARCHAR(50), bid2_size VARCHAR(50),
                            bid3_price VARCHAR(50), bid3_size VARCHAR(50),
                            bid4_price VARCHAR(50), bid4_size VARCHAR(50),
                            bid5_price VARCHAR(50), bid5_size VARCHAR(50),
                        
                            ask1_price VARCHAR(50), ask1_size VARCHAR(50),
                            ask2_price VARCHAR(50), ask2_size VARCHAR(50),
                            ask3_price VARCHAR(50), ask3_size VARCHAR(50),
                            ask4_price VARCHAR(50), ask4_size VARCHAR(50),
                            ask5_price VARCHAR(50), ask5_size VARCHAR(50)
                        )
                    """)


    async def record_order(self, order: DatabaseOrder):
        async with self.pool.acquire() as connection:
            async with connection.cursor() as cursor:
                await cursor.execute("""
                    INSERT INTO orders (pair, side, quantity, price, timestamp)
                    VALUES (%s, %s, %s, %s, %s)
                """, (order.pair, order.side, str(order.size), str(order.price), order.timestamp))


    async def record_market_state(self, market_state: DatabaseMarketState):
        async with self.pool.acquire() as connection:
            async with connection.cursor() as cursor:
                await cursor.execute("""
                    INSERT INTO market_states  (market_depth, fair_price, market_spread, usdt_balance, rmv_balance, rmv_value, timestamp)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                """, (str(market_state.market_depth), str(market_state.fair_price), str(market_state.market_spread), str(market_state.usdt_balance), str(market_state.rmv_balance), str(market_state.rmv_value), str(market_state.timestamp)))

    async def record_orderbook(self, table: str, exchange: str, orderbook: OrderBook, timestamp: str):
        values = []
        for i in range(5):
            if i < len(orderbook.bids):
                level = orderbook.bids[i]
                values.extend([str(level.price), str(level.size)])
            else:
                values.extend([None, None])
        for i in range(5):
            if i < len(orderbook.asks):
                level = orderbook.asks[i]
                values.extend([str(level.price), str(level.size)])
            else:
                values.extend([None, None])

        async with self.pool.acquire() as connection:
            async with connection.cursor() as cursor:
                await cursor.execute(f"""
                    INSERT INTO {table} (exchange, symbol, timestamp, bid1_price, bid1_size, bid2_price, bid2_size, bid3_price, bid3_size, bid4_price, bid4_size, bid5_price, bid5_size, ask1_price, ask1_size, ask2_price, ask2_size, ask3_price, ask3_size, ask4_price, ask4_size, ask5_price, ask5_size)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (exchange, "RMV-USDT", timestamp, *values))



    async def close(self):
        if self.pool:
            self.pool.close()
            await self.pool.wait_closed()
            logger.info("Database pool closed")