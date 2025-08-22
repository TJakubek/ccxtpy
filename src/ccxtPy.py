import asyncio
import signal
import time
import sys
from datetime import datetime
from collections import defaultdict
from typing import List
import os
from dotenv import load_dotenv

import ccxt.pro as ccxt
from pymongo import MongoClient
import logging

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# MongoDB settings
MONGO_URL = os.getenv("MONGO_SERVER", "mongodb://localhost:27017")
DB_NAME = "tso"
general_source_id = 'ccxtPy'

TOKENS = [
    "BTC", "ETH", "BNB", "XRP", "ADA", "SOL", "DOGE", "DOT", "AVAX", "SHIB",
    "MATIC", "LTC", "UNI", "LINK", "ATOM", "ETC", "XLM", "BCH", "ALGO", "VET",
    "ICP", "FIL", "TRX", "APE", "NEAR", "MANA", "SAND", "AXS", "THETA", "AAVE"
]
QUOTE_CURRENCIES = ["USD", "USDT", "USDC"]
EXCHANGES = [
    'ascendex','bequant','binance','binanceus','binanceusdm','bingx','bitfinex','bitfinex1',
    'bitget','bitmart','bitmex','bitrue','bitstamp','blockchaincom','bybit','cryptocom',
    'deribit','exmo','gate','gateio','hashkey','hitbtc','kraken','kucoin','myokx',
    'p2b','phemex','poloniex','probit','upbit','whitebit','woo','xt',
]

# --- Get exchange from CLI ---
if len(sys.argv) < 2:
    logger.error("❌ Please provide the exchange name as the first argument.")
    sys.exit(1)

selected_exchange = sys.argv[1].lower()
if selected_exchange not in EXCHANGES:
    logger.error(f"❌ Exchange '{selected_exchange}' is not supported.")
    sys.exit(1)

EXCHANGES = [selected_exchange]

class PriceTracker:
    def __init__(self):
        self.price_data = defaultdict(lambda: defaultdict(dict))
        self.exchanges = {}
        self.client = None
        self.price_collections = {}
        self.shutdown_event = asyncio.Event()
        self.tasks = []
        self.lock = asyncio.Lock()
        self.trackedStuff = []

    def current_timestamp(self):
        return round(datetime.utcnow().timestamp())

    async def initialize(self):
        logger.info("Initializing Price Tracker...")
        try:
            self.client = MongoClient(MONGO_URL)
            self.client.admin.command('ping')
            logger.info("✅ MongoDB connected")
        except Exception as e:
            logger.error(f"❌ MongoDB connection failed: {e}")
            raise

        successful_exchanges = []
        for exchange_id in EXCHANGES:
            try:
                exchange_class = getattr(ccxt, exchange_id)
                exchange = exchange_class({
                    'sandbox': False,
                    'enableRateLimit': True,
                })
                self.exchanges[exchange_id] = exchange
                successful_exchanges.append(exchange_id)
                logger.info(f"✅ Initialized {exchange_id}")
            except Exception as e:
                logger.error(f"❌ {exchange_id} init failed: {e}")

        return len(successful_exchanges) > 0

    async def get_available_pairs(self, exchange, exchange_id: str) -> List[str]:
        try:
            await exchange.load_markets()
            available_pairs = []

            for token in TOKENS:
                for quote in QUOTE_CURRENCIES:
                    symbol = f"{token}/{quote}"
                    if symbol in exchange.markets:
                        available_pairs.append({"base": token, "quote": quote, "market": symbol})

            logger.info(f"📊 {exchange_id}: {len(available_pairs)} pairs")
            return available_pairs

        except Exception as e:
            logger.error(f"❌ {exchange_id} market load failed: {e}")
            return []

    async def watch_ticker(self, exchange, exchange_id: str, symbol: str):
        try:
            await exchange.watch_ticker(symbol)
            self.trackedStuff.append({"exchange": exchange, "method": "ticker", "symbol": symbol})
        except Exception as e:
            logger.error(f"❌ Ticker watch failed {exchange_id} {symbol}: {e}")

    async def watch_orderbook(self, exchange, exchange_id: str, symbol: str):
        try:
            if not exchange.has.get('watchOrderBook'):
                return
            limit = {"bitfinex": 25, "bitmex": 25}.get(exchange.id, 20)
            await exchange.watch_order_book(symbol, limit)
            self.trackedStuff.append({"exchange": exchange, "method": "orderbook", "symbol": symbol})
        except Exception as e:
            logger.error(f"❌ Orderbook watch failed {exchange_id} {symbol}: {e}")

    async def start_exchange_watchers(self, exchange, exchange_id: str):
        pairs = await self.get_available_pairs(exchange, exchange_id)
        if not pairs:
            logger.warning(f"⚠️ No pairs for {exchange_id}")
            return

        for pair in pairs:
            symbol = pair['market']
            if exchange.has.get('watchTicker'):
                self.tasks.append(asyncio.create_task(self.watch_ticker(exchange, exchange_id, symbol)))
            if exchange.has.get('watchOrderBook'):
                self.tasks.append(asyncio.create_task(self.watch_orderbook(exchange, exchange_id, symbol)))

    async def save_to_mongo_minimal_test(self):
        logger.info("💾 MongoDB save started")
        loop_counter = 0
        start_time = time.time()

        while not self.shutdown_event.is_set():
            loop_counter += 1
            current_ts = self.current_timestamp()
            drift = time.time() - (start_time + loop_counter)
            print(f"Loop {loop_counter}: {current_ts}, tracked={len(self.trackedStuff)}, drift={drift:.3f}s")

            sleep_time = 1.0 - drift
            await asyncio.sleep(max(0.001, sleep_time))

    async def run(self):
        if not await self.initialize():
            logger.error("❌ Initialization failed")
            return

        try:
            for exchange_id, exchange in self.exchanges.items():
                await self.start_exchange_watchers(exchange, exchange_id)

            self.tasks.append(asyncio.create_task(self.save_to_mongo_minimal_test()))
            logger.info("🚀 All systems running")
            await asyncio.gather(*self.tasks, return_exceptions=True)
        finally:
            await self.cleanup()

    async def cleanup(self):
        logger.info("🧹 Cleaning up...")
        self.shutdown_event.set()

        for task in self.tasks:
            if not task.done():
                task.cancel()

        await asyncio.gather(*self.tasks, return_exceptions=True)

        if self.exchanges:
            await asyncio.gather(*(ex.close() for ex in self.exchanges.values()), return_exceptions=True)

        if self.client:
            self.client.close()

        logger.info("✅ Cleanup done")

# --- Signal-safe wrapper ---
tracker = None

def sync_signal_handler():
    logger.info("⚠️ Received termination signal")
    if tracker:
        asyncio.create_task(tracker.cleanup())

async def main():
    global tracker
    tracker = PriceTracker()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, sync_signal_handler)

    try:
        await tracker.run()
    except Exception as e:
        logger.error(f"🚨 Fatal error: {e}")
    finally:
        logger.info("🛑 Script terminated")

if __name__ == "__main__":
    asyncio.run(main())
