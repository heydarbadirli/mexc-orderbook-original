# MEXC Market-Making Bot

A python bot for placing orders on both sides of orderbook to meet the MEXC requirements: market-spread < 2%, market-depth > 1000 USDT within 2% from mid-price


Code explanation:
There is event queue where different functions send information, depends on information what part of code will be executed, all types are in model.py

mexc/client.py contains functions related to MEXC api like getting orderbook, tracking orders etc
kucoin/client.oy contains functions related to KuCoin api like getting orderbook
market/tracking.py contains 3 function:
1) reset_orders: resets orders every 30 minutes to be sure that we are keeping right state
2) manage_orders: cancels orders that are incorrect (too big size, bad price, etc.) and adds new orders to always have some number of bids and asks: we can set that number in code, basically it is 5
3) check_market_depth: checks if market depth is as expected, if not it iterates through our active orders and add volume, how many add first currency and second is calculated based on ratio

There are several functions running concurrently
1) updating kucoin_orderbook via websockets
2) updating mexc_orderbook via websockets
3) tracking active orders via websockets (all information about new orders, ones that were cancelled/filled)
4) extending listen key (necessary for other mexc functions to work, listen key expires after 60 minutes)
5) tracking balance on mexc via websockets
6) reading from event queue

