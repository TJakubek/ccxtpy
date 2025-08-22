import os
import asyncio
import ccxt.pro as ccxt
from pymongo import MongoClient
from dotenv import load_dotenv
from collections import defaultdict
from datetime import datetime
import signal

# Load environment variables
load_dotenv()

# Constants
MONGO_URL = os.getenv("MONGO_SERVER")
DB_NAME = "tso"
COLLECTION_NAME = "symbols"
general_source_id = 'ccxtPy'
quote_list = ["USD", "USDT", "USD:USD", "USDT:USDT"]

EXCHANGES = [
    'ascendex', 'bequant', 'binance', 'binanceus', 'binanceusdm', 'bingx',
    'bitfinex', 'bitfinex1', 'bitget', 'bitmart', 'bitmex', 'bitopro',
    'bitrue', 'bitstamp', 'blockchaincom', 'blofin', 'bybit', 'cryptocom',
    'deribit', 'exmo', 'gate', 'gateio', 'gemini', 'hashkey', 'hitbtc',
    'htx', 'huobi', 'kraken', 'krakenfutures', 'kucoin', 'myokx', 'okx',
    'p2b', 'phemex', 'poloniex', 'poloniexfutures', 'probit', 'upbit',
    'whitebit', 'woo', 'xt']

# Price Store
class PriceStore:
    def __init__(self):
        self.store = defaultdict(dict)
        self.general_source_id = 'ccxtPy'
        self.blacklist = {'LEO': ['mexc'], 'TRUMP': ['poloniex', 'phemex', 'bitmex']}
        self.price_collections = {}
        self.client = None
        self._saving = False
        self._save_task = None

    def update_price(self, token, source_id, price):
        if self.is_blacklisted(token, source_id):
            return
        if not self._saving:
            self._save_task = asyncio.create_task(self._save_loop())
            self._saving = True
        self.store[token][source_id] = float(price)

    async def _save_loop(self):
        while True:
            await asyncio.sleep(1)
            timestamp = self.current_timestamp()
            for token, prices in list(self.store.items()):
                if not prices:
                    continue
                formatted = [{'sourceId': sid, 'price': price} for sid, price in prices.items()]
                collection = self.get_price_collection(token)
                # collection.update_one({'timestamp': timestamp}, {'$set': {'prices': formatted}}, upsert=True)
                print(f"{token}: saved {len(formatted)} prices")
                self.store[token].clear()

    def get_price_collection(self, symbol):
        if not self.client:
            self.client = MongoClient(MONGO_URL)
        db = self.client[DB_NAME]
        if symbol not in self.price_collections:
            self.price_collections[symbol] = db[f"prices-{symbol}"]
        return self.price_collections[symbol]

    async def close(self):
        if self._save_task:
            self._save_task.cancel()
            try:
                await self._save_task
            except asyncio.CancelledError:
                pass
        if self.client:
            self.client.close()

    @staticmethod
    def current_timestamp():
        return round(datetime.utcnow().timestamp())

    def is_blacklisted(self, token, source_id):
        return any(b in source_id.lower() for b in self.blacklist.get(token, []))

# Instantiate
price_store = PriceStore()

# Signal handling
def shutdown():
    for task in asyncio.all_tasks():
        task.cancel()

def setup_signals():
    loop = asyncio.get_event_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, shutdown)

# Async Helpers
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
    limit = {"bitfinex": 25, "bitfinex1": 25, "bitmex": 25, "bybit": 1, "kraken": 10, "poloniexfutures": 5}.get(exchange.id, 20)
    while True:
        try:
            ob = await exchange.watch_order_book(symbol, limit)
            if ob.get("asks") and ob.get("bids"):
                ask, bid = ob["asks"][0][0], ob["bids"][0][0]
                if isinstance(ask, (int, float)) and isinstance(bid, (int, float)):
                    mid = (ask + bid) / 2
                    price_store.update_price(base_token, f"{general_source_id}_{exchange.id}_{symbol}_ob", mid)
        except Exception as e:
            print(f"⚠️ OB error on {exchange.id} {symbol}: {e}")
            await asyncio.sleep(5)

async def watch_trades(exchange, symbol, base_token):
    if not exchange.has.get("watchTrades"):
        return
    while True:
        try:
            trades = await exchange.watch_trades(symbol)
            if trades:
                last = max(trades, key=lambda x: x["timestamp"])
                price_store.update_price(base_token, f"{general_source_id}_{exchange.id}_{symbol}_trade", float(last["price"]))
        except Exception as e:
            print(f"⚠️ Trade error on {exchange.id} {symbol}: {e}")
            await asyncio.sleep(5)

async def watch_ticker(exchange, symbol, base_token):
    if not exchange.has.get("watchTicker"):
        return
    while True:
        try:
            t = await exchange.watch_ticker(symbol)
            if t:
                if "last" in t:
                    price_store.update_price(base_token, f"{general_source_id}_{exchange.id}_{symbol}_last", float(t["last"]))
                if "average" in t:
                    price_store.update_price(base_token, f"{general_source_id}_{exchange.id}_{symbol}_avg", float(t["average"]))
        except Exception as e:
            print(f"⚠️ Ticker error on {exchange.id} {symbol}: {e}")
            await asyncio.sleep(5)

async def fetch_markets_with_usd(exchange, symbols):
    await exchange.load_markets()
    matches = {}
    for sym in symbols:
        matches[sym] = []
        for q in quote_list:
            full = f"{sym}/{q}"
            if full in exchange.markets:
                base, quote = full.split("/")
                if base in symbols:
                    matches[sym].append({"base": base, "quote": quote, "market": exchange.markets[full]})
    return matches

async def watch_for_exchange(exchange, markets, symbols):
    tasks = []
    for symbol, mlist in markets.items():
        for data in mlist:
            base = data["base"]
            sym = data["market"]["symbol"]
            if base in symbols:
                tasks += [asyncio.create_task(watch_order_books(exchange, sym, base)),
                          asyncio.create_task(watch_trades(exchange, sym, base)),
                          asyncio.create_task(watch_ticker(exchange, sym, base))]
    if tasks:
        await asyncio.gather(*tasks)

async def main():
    symbols = await fetch_symbols_from_mongo()
    exchange_instances = {eid: getattr(ccxt, eid)() for eid in EXCHANGES}
    try:
        results = await asyncio.gather(
            *[fetch_markets_with_usd(exchange_instances[eid], symbols) for eid in EXCHANGES])
        watchers = [asyncio.create_task(watch_for_exchange(exchange_instances[eid], markets, symbols))
                    for eid, markets in zip(EXCHANGES, results)]
        await asyncio.gather(*watchers)
    except asyncio.CancelledError:
        print("⚠️ Cancelled")
    finally:
        print("🔄 Cleaning up...")
        await price_store.close()
        for ex in exchange_instances.values():
            try:
                await ex.close()
                if getattr(ex, 'session', None):
                    await ex.session.close()
            except Exception as e:
                print(f"⚠️ Error closing {ex.id}: {e}")

if __name__ == "__main__":
    setup_signals()
    try:
        asyncio.run(main())
    except Exception as e:
        print(f"🚨 Unhandled error: {e}")
    finally:
        print("🛑 Shutdown complete")
