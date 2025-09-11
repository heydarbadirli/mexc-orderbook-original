from loguru import logger
from src.model import DatabaseOrder, DatabaseMarketState, OrderBook
import aiomysql
from decimal import Decimal, ROUND_HALF_UP

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

        for table_name in ['orders', 'every_order_placed']:
            async with self.pool.acquire() as connection:
                async with connection.cursor() as cursor:
                    await cursor.execute(f"""
                        CREATE TABLE IF NOT EXISTS {table_name} (
                            id INT AUTO_INCREMENT PRIMARY KEY,
                            pair VARCHAR(50),
                            side VARCHAR(50),
                            quantity DECIMAL(20,8),
                            price DECIMAL(20,8),
                            order_id VARCHAR(50),
                            timestamp DATETIME
                        )
                    """)

        async with self.pool.acquire() as connection:
            async with connection.cursor() as cursor:
                await cursor.execute("""
                    CREATE TABLE IF NOT EXISTS market_states (
                        id INT AUTO_INCREMENT PRIMARY KEY,
                        market_depth DECIMAL(20,8),
                        fair_price DECIMAL(20,8),
                        market_spread DECIMAL(20,8),
                        usdt_balance DECIMAL(20,8),
                        rmv_balance DECIMAL(20,8),
                        rmv_value DECIMAL(20,8),
                        timestamp DATETIME
                    )
                """)


        for table_name in ['kucoin_orderbook', 'mexc_orderbook', 'our_orders']:
            async with self.pool.acquire() as connection:
                async with connection.cursor() as cursor:
                    await cursor.execute(f"""
                        CREATE TABLE IF NOT EXISTS {table_name} (
                            id INT AUTO_INCREMENT PRIMARY KEY,
                            exchange VARCHAR(50),
                            symbol VARCHAR(50),
                            timestamp DATETIME,
                            
                            bid1_price DECIMAL(20,8), bid1_size DECIMAL(20,8),
                            bid2_price DECIMAL(20,8), bid2_size DECIMAL(20,8),
                            bid3_price DECIMAL(20,8), bid3_size DECIMAL(20,8),
                            bid4_price DECIMAL(20,8), bid4_size DECIMAL(20,8),
                            bid5_price DECIMAL(20,8), bid5_size DECIMAL(20,8),
                        
                            ask1_price DECIMAL(20,8), ask1_size DECIMAL(20,8),
                            ask2_price DECIMAL(20,8), ask2_size DECIMAL(20,8),
                            ask3_price DECIMAL(20,8), ask3_size DECIMAL(20,8),
                            ask4_price DECIMAL(20,8), ask4_size DECIMAL(20,8),
                            ask5_price DECIMAL(20,8), ask5_size DECIMAL(20,8)
                        )
                    """)


    async def record_order(self, order: DatabaseOrder, table_name: str):
        if table_name == 'orders':
            logger.info(f'Recording order: {order}')

        async with self.pool.acquire() as connection:
            try:
                async with connection.cursor() as cursor:
                    await cursor.execute(f"""
                        INSERT INTO {table_name} (pair, side, quantity, price, timestamp, order_id)
                        VALUES (%s, %s, %s, %s, %s, %s)
                    """, (order.pair, order.side, order.size, order.price, order.timestamp, order.order_id))
            except Exception as e:
                logger.error(f"Failed to record order: {e}")


    async def record_market_state(self, market_state: DatabaseMarketState):
        async with self.pool.acquire() as connection:
            async with connection.cursor() as cursor:
                try:
                    await cursor.execute("""
                        INSERT INTO market_states  (market_depth, fair_price, market_spread, usdt_balance, rmv_balance, rmv_value, timestamp)
                        VALUES (%s, %s, %s, %s, %s, %s, %s)
                    """, (market_state.market_depth.quantize(Decimal('0.00000001'), rounding=ROUND_HALF_UP), market_state.fair_price.quantize(Decimal('0.00000001'), rounding=ROUND_HALF_UP), market_state.market_spread.quantize(Decimal('0.00000001'), rounding=ROUND_HALF_UP), market_state.usdt_balance.quantize(Decimal('0.00000001'), rounding=ROUND_HALF_UP), market_state.rmv_balance.quantize(Decimal('0.00000001'), rounding=ROUND_HALF_UP), market_state.rmv_value.quantize(Decimal('0.00000001'), rounding=ROUND_HALF_UP), market_state.timestamp))
                except Exception as e:
                    logger.error(f"Failed to record market state: {e}")


    async def record_orderbook(self, table: str, exchange: str, orderbook: OrderBook, timestamp: str):
        values = []
        for i in range(5):
            if i < len(orderbook.bids):
                level = orderbook.bids[i]
                values.extend([level.price, level.size])
            else:
                values.extend([None, None])
        for i in range(5):
            if i < len(orderbook.asks):
                level = orderbook.asks[i]
                values.extend([level.price, level.size])
            else:
                values.extend([None, None])

        async with self.pool.acquire() as connection:
            async with connection.cursor() as cursor:
                try:
                    await cursor.execute(f"""
                        INSERT INTO {table} (exchange, symbol, timestamp, bid1_price, bid1_size, bid2_price, bid2_size, bid3_price, bid3_size, bid4_price, bid4_size, bid5_price, bid5_size, ask1_price, ask1_size, ask2_price, ask2_size, ask3_price, ask3_size, ask4_price, ask4_size, ask5_price, ask5_size)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """, (exchange, "RMV-USDT", timestamp, *values))
                except Exception as e:
                    logger.error(f"Failer to record {exchange} orderbook: {e}")


    async def close(self):
        if self.pool:
            self.pool.close()
            await self.pool.wait_closed()
            logger.info("Database pool closed")