import os
import asyncio
import ccxt.pro as ccxt
from motor.motor_asyncio import AsyncIOMotorClient
from dotenv import load_dotenv
from datetime import datetime
import signal

# Load environment variables
load_dotenv()

MONGO_URL = os.getenv("MONGO_SERVER")
DB_NAME = "tso"
EXCHANGES = ['binance', 'kraken', 'okx']  # Add more as needed

class PriceStore:
    def __init__(self):
        self.store = {}
        self.client = AsyncIOMotorClient(MONGO_URL)
        self.db = self.client[DB_NAME]
        self.running = True

    def update_price(self, token, source_id, price):
        if price is not None and isinstance(price, (int, float)):
            self.store.setdefault(token, {})[source_id] = price

    async def save(self):
        """Save to MongoDB every second."""
        while self.running:
            if self.store:
                updates = [
                    {"timestamp": datetime.utcnow().timestamp(), "token": token, "prices": sources}
                    for token, sources in self.store.items()
                ]
                await self.db["price_store"].insert_many(updates)
                print(f"✅ Saved {len(updates)} updates to MongoDB")
                self.store.clear()
            await asyncio.sleep(1)

    async def close(self):
        self.running = False
        self.client.close()

async def fetch_symbols():
    """Fetch unique symbols from MongoDB."""
    client = AsyncIOMotorClient(MONGO_URL)
    db = client[DB_NAME]
    symbols = await db["symbols"].distinct("symbol")
    client.close()
    return list(set(symbols))

async def watch_data(exchange, symbol, base_token, price_store):
    async def watch_order_books():
        if not exchange.has.get("watchOrderBook"):
            return
        while True:
            try:
                orderbook = await exchange.watch_order_book(symbol)
                if orderbook and orderbook.get("asks") and orderbook.get("bids"):
                    mid_price = (orderbook["asks"][0][0] + orderbook["bids"][0][0]) / 2
                    price_store.update_price(base_token, f"{exchange.id}_{symbol}_ob", mid_price)
            except Exception as e:
                print(f"⚠️ Order book error on {exchange.id} {symbol}: {e}")
                await asyncio.sleep(5)

    async def watch_trades():
        if not exchange.has.get("watchTrades"):
            return
        while True:
            try:
                trades = await exchange.watch_trades(symbol)
                if trades:
                    trade_price = max(trades, key=lambda x: x["timestamp"])["price"]
                    price_store.update_price(base_token, f"{exchange.id}_{symbol}_trade", trade_price)
            except Exception as e:
                print(f"⚠️ Trade error on {exchange.id} {symbol}: {e}")
                await asyncio.sleep(5)

    async def watch_ticker():
        if not exchange.has.get("watchTicker"):
            return
        while True:
            try:
                ticker = await exchange.watch_ticker(symbol)
                if ticker and "last" in ticker:
                    price_store.update_price(base_token, f"{exchange.id}_{symbol}_last", ticker["last"])
            except Exception as e:
                print(f"⚠️ Ticker error on {exchange.id} {symbol}: {e}")
                await asyncio.sleep(5)

    await asyncio.gather(watch_order_books(), watch_trades(), watch_ticker())

async def main():
    symbols = await fetch_symbols()
    exchange_instances = {eid: getattr(ccxt, eid)() for eid in EXCHANGES}
    price_store = PriceStore()
    save_task = asyncio.create_task(price_store.save())

    try:
        # Load markets for all exchanges
        results = await asyncio.gather(*[exchange.load_markets() for exchange in exchange_instances.values()])

        tasks = []
        for eid, exchange in exchange_instances.items():
            for symbol in symbols:
                market_symbol = f"{symbol}/USDT"
                if market_symbol in exchange.markets:
                    tasks.append(watch_data(exchange, market_symbol, symbol, price_store))

        await asyncio.gather(*tasks)

    except asyncio.CancelledError:
        print("\n⚠️ Script interrupted. Cleaning up...")

    finally:
        print("🔄 Closing exchanges and resources...")
        save_task.cancel()
        await price_store.close()
        for exchange in exchange_instances.values():
            await exchange.close()

def signal_handler(sig, frame):
    print("\n⚠️ Interrupt signal received! Shutting down...")
    for task in asyncio.all_tasks():
        task.cancel()

signal.signal(signal.SIGINT, signal_handler)

if __name__ == "__main__":
    asyncio.run(main())
