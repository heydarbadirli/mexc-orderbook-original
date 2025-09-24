# MEXC Market-Making Bot

A python bot for placing orders on both sides of orderbook to meet the MEXC requirements: market-spread < 2%, market-depth > 1000 USDT within 2% from mid-price


Code explanation:
There is event queue where different functions send information, depends on information what part of code will be executed, all types are in model.py

mexc/client.py contains functions related to MEXC api like getting orderbook, tracking orders etc
kucoin/client.oy contains functions related to KuCoin api like getting orderbook
market/tracking.py contains 3 function:
1) reset_orders: resets orders every 30 minutes to be sure that we are keeping right state
2) manage_orders: cancels orders that are incorrect (too big size, bad price, etc.) and adds new orders to always have some number of bids and asks: we can set that number in code, basically it is 5
3) check_market_depth: checks if market depth is as expected, if not it iterates through our active orders and add volume, how many add first currency and second currency is calculated based on balance ratio

market/calculations.py contains functions related to calculate different parameters of market
1) calculate_market_depth: calculates market depth of the market within some percent that we can set
2) calculate_fair_price: calculates fair price using formula for cross-exchanged fair price, we can set if we want to include our orders in calculating liquidity and mid-price
3) calculate_market_spread: calculates marked spread
4) get_quotes: calculates at which price we should place the lowest ask and the highest bid

database.py contains functions used to recording data in database
records mexc orderbook, kucoin orderbook, orders that were filled, all orders that we have placed

There are several functions running concurrently
1) updating kucoin_orderbook via websockets
2) updating mexc_orderbook via websockets
3) tracking active orders via websockets (all information about new orders, ones that were cancelled/filled)
4) extending listen key (necessary for other mexc functions to work, listen key expires after 60 minutes)
5) tracking balance on mexc via websockets
6) reading from event queue
