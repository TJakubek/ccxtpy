import os
import asyncio
import ccxt.pro as ccxt
from pymongo import MongoClient
from dotenv import load_dotenv
from priceStore import PriceStore
import signal
from datetime import datetime

# Load environment variables from .env file
load_dotenv()

# MongoDB Configuration
MONGO_URL = os.getenv("MONGO_SERVER")
DB_NAME = "tso"
COLLECTION_NAME = "symbols"
general_source_id = 'ccxtPy'

# List of Exchanges
EXCHANGES = ['ascendex', 'binance', 'bitfinex', 'bybit', 'kraken', 'kucoin', 'okx', 'phemex', 'poloniex', 'upbit']  # Adjust as needed

quote_list = ["USD", "USDT", "USD:USD", "USDT:USDT"]
price_store = PriceStore()

def get_exchange_instances():
    return {eid: getattr(ccxt, eid)() for eid in EXCHANGES}

async def fetch_symbols_from_mongo():
    client = MongoClient(MONGO_URL)
    db = client[DB_NAME]
    collection = db[COLLECTION_NAME]
    symbols = list(set(collection.distinct("symbol")))
    client.close()
    return symbols

async def watch_order_books(exchange, symbol, base_token):
    if not exchange.has.get("watchOrderBook"):
        return
    
    while True:
        try:
            orderbook = await exchange.watch_order_book(symbol)
            if orderbook and orderbook.get("asks") and orderbook.get("bids"):
                ask_price = orderbook["asks"][0][0]
                bid_price = orderbook["bids"][0][0]
                mid_price = (ask_price + bid_price) / 2
                price_store.update_price(base_token, f"{general_source_id}_{exchange.id}_{symbol}_ob", mid_price)
        except Exception as e:
            print(f"⚠️ Order book error on {exchange.id} {symbol}: {e}")
            await asyncio.sleep(5)

async def watch_trades(exchange, symbol, base_token):
    if not exchange.has.get("watchTrades"):
        return
    
    while True:
        try:
            trades = await exchange.watch_trades(symbol)
            if trades:
                latest_trade = max(trades, key=lambda x: x["timestamp"])
                trade_price = latest_trade.get("price")
                if trade_price is not None:
                    price_store.update_price(base_token, f"{general_source_id}_{exchange.id}_{symbol}_trade", float(trade_price))
        except Exception as e:
            print(f"⚠️ Trade error on {exchange.id} {symbol}: {e}")
            await asyncio.sleep(5)

async def watch_ticker(exchange, symbol, base_token):
    if not exchange.has.get("watchTicker"):
        return
    
    while True:
        try:
            ticker = await exchange.watch_ticker(symbol)
            if ticker and "last" in ticker:
                price_store.update_price(base_token, f"{general_source_id}_{exchange.id}_{symbol}_last", float(ticker["last"]))
        except Exception as e:
            print(f"⚠️ Ticker error on {exchange.id} {symbol}: {e}")
            await asyncio.sleep(5)

async def watch_for_exchange(exchange, symbols):
    tasks = []
    
    for symbol in symbols:
        tasks.append(watch_order_books(exchange, symbol, symbol))
        tasks.append(watch_trades(exchange, symbol, symbol))
        tasks.append(watch_ticker(exchange, symbol, symbol))
    
    await asyncio.gather(*tasks)

async def periodic_save():
    while True:
        price_store.save()
        await asyncio.sleep(1)

async def main():
    symbols = await fetch_symbols_from_mongo()
    exchange_instances = get_exchange_instances()
    
    tasks = [watch_for_exchange(exchange_instances[eid], symbols) for eid in EXCHANGES]
    tasks.append(periodic_save())
    
    try:
        await asyncio.gather(*tasks)
    except asyncio.CancelledError:
        print("\n⚠️ Script interrupted. Cleaning up...")
    finally:
        price_store.close()
        for exchange in exchange_instances.values():
            await exchange.close()
        print("✅ All exchanges closed successfully.")

def signal_handler(sig, frame):
    print("\n⚠️ Interrupt signal received! Shutting down...")
    for task in asyncio.all_tasks():
        task.cancel()

signal.signal(signal.SIGINT, signal_handler)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n❌ Script interrupted by user.")
    except Exception as e:
        print(f"🚨 Unexpected error: {e}")
    finally:
        price_store.close()
