import asyncio
import signal
import time
from datetime import datetime
from collections import defaultdict
from typing import List
import os
import sys
import argparse
from dotenv import load_dotenv
import ccxt.pro as ccxt
from pymongo import MongoClient
import logging

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Configuration
MONGO_URL = os.getenv("MONGO_SERVER", "mongodb://localhost:27017")
DB_NAME = "tso"
TOKENS = [
    "BTC", "ETH", "BNB", "XRP", "ADA", "SOL", "DOGE", "DOT", "AVAX", "SHIB",
    "MATIC", "LTC", "UNI", "LINK", "ATOM", "ETC", "XLM", "BCH", "ALGO", "VET",
    "ICP", "FIL", "TRX", "APE", "NEAR", "MANA", "SAND", "AXS", "THETA", "AAVE"
]
QUOTE_CURRENCIES = ["USD", "USDT", "USDC"]

class PriceTracker:
    def __init__(self, exchange_id: str):
        self.price_data = defaultdict(lambda: defaultdict(dict))
        self.exchange_id = exchange_id
        self.exchange = None
        self.client = None
        self.price_collections = {}
        self.shutdown_event = asyncio.Event()
        self.tasks = []
        self.trackedStuff = []

    def current_timestamp(self):
        return round(datetime.utcnow().timestamp())

    async def initialize(self):
        logger.info(f"Initializing {self.exchange_id}...")
        try:
            self.client = MongoClient(MONGO_URL)
            self.client.admin.command('ping')
            logger.info("✅ MongoDB connected")
        except Exception as e:
            logger.error(f"❌ MongoDB error: {e}")
            raise

        try:
            exchange_class = getattr(ccxt, self.exchange_id)
            self.exchange = exchange_class({
                'sandbox': False,
                'enableRateLimit': True,
            })
            logger.info(f"✅ Initialized exchange: {self.exchange_id}")
        except Exception as e:
            logger.error(f"❌ Failed to initialize exchange {self.exchange_id}: {e}")
            raise

    async def get_available_pairs(self) -> List[str]:
        try:
            await self.exchange.load_markets()
            available_pairs = []
            for token in TOKENS:
                for quote in QUOTE_CURRENCIES:
                    symbol = f"{token}/{quote}"
                    if symbol in self.exchange.markets:
                        available_pairs.append(symbol)
            return available_pairs
        except Exception as e:
            logger.error(f"❌ Error loading markets: {e}")
            return []

    async def watch_ticker(self, symbol: str):
        try:
            await self.exchange.watch_ticker(symbol)
            self.trackedStuff.append({"exchange": self.exchange, "method": "ticker", "symbol": symbol})
        except Exception as e:
            logger.error(f"❌ Ticker watch error for {symbol}: {e}")

    async def watch_orderbook(self, symbol: str):
        try:
            if not self.exchange.has.get('watchOrderBook'):
                return
            limit = 20
            await self.exchange.watch_order_book(symbol, limit)
            self.trackedStuff.append({"exchange": self.exchange, "method": "orderbook", "symbol": symbol})
        except Exception as e:
            logger.error(f"❌ Orderbook watch error for {symbol}: {e}")

    async def start_watchers(self):
        pairs = await self.get_available_pairs()
        if not pairs:
            logger.warning("⚠️ No valid pairs found")
            return

        for symbol in pairs:
            if self.exchange.has.get('watchTicker'):
                self.tasks.append(asyncio.create_task(self.watch_ticker(symbol)))
            if self.exchange.has.get('watchOrderBook'):
                self.tasks.append(asyncio.create_task(self.watch_orderbook(symbol)))

        logger.info(f"👀 Watching {len(self.tasks)} feeds...")
        await asyncio.gather(*self.tasks, return_exceptions=True)

    async def save_to_mongo_minimal_test(self):
        logger.info("💾 Starting MongoDB minimal save task")
        loop_counter = 0
        start_time = time.time()
        while not self.shutdown_event.is_set():
            loop_counter += 1
            current_ts = self.current_timestamp()
            tracked = len(self.trackedStuff)
            drift = (time.time() - start_time) - (loop_counter * 1.0)
            print(f"Loop #{loop_counter}: {current_ts}, tracked={tracked}, drift={drift:.3f}s")
            await asyncio.sleep(max(0.0, start_time + (loop_counter * 1.0) - time.time()))

    async def cleanup(self):
        logger.info("🧹 Cleaning up...")
        self.shutdown_event.set()
        for task in self.tasks:
            task.cancel()
        await asyncio.gather(*self.tasks, return_exceptions=True)
        if self.exchange:
            await self.exchange.close()
        if self.client:
            self.client.close()
        logger.info("✅ Cleanup done")

    async def run(self):
        await self.initialize()
        watcher_task = asyncio.create_task(self.start_watchers())
        save_task = asyncio.create_task(self.save_to_mongo_minimal_test())
        self.tasks.extend([watcher_task, save_task])
        await asyncio.wait([watcher_task, save_task], return_when=asyncio.FIRST_COMPLETED)

# ----- Exit Handling -----

tracker = None

def signal_handler(signum, frame):
    logger.info(f"⚠️ Signal {signum} received")
    if tracker:
        asyncio.create_task(tracker.cleanup())

async def main():
    global tracker

    parser = argparse.ArgumentParser()
    parser.add_argument("--exchange", type=str, required=True, help="Exchange ID (e.g. binance)")
    args = parser.parse_args()

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    # Exit when parent process dies (for TS -> Python kill detection)
    def monitor_parent():
        import psutil
        parent_pid = os.getppid()
        while True:
            if not psutil.pid_exists(parent_pid):
                logger.warning("🛑 Parent process exited. Shutting down...")
                os.kill(os.getpid(), signal.SIGTERM)
            time.sleep(2)

    import threading
    threading.Thread(target=monitor_parent, daemon=True).start()

    tracker = PriceTracker(args.exchange)

    try:
        await tracker.run()
    finally:
        await tracker.cleanup()

if __name__ == "__main__":
    import psutil  # required for parent monitor
    asyncio.run(main())
