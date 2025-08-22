import asyncio
import signal
import time
import sys
from datetime import datetime
from collections import defaultdict
from typing import Dict, List, Set
import os
from dotenv import load_dotenv

import ccxt.pro as ccxt
from pymongo import MongoClient
import logging

# Load environment variables
load_dotenv()

# Global variable to store exchange ID for logging
CURRENT_EXCHANGE_ID = None

class ExchangeFormatter(logging.Formatter):
    def format(self, record):
        if CURRENT_EXCHANGE_ID:
            record.exchange = f"[{CURRENT_EXCHANGE_ID}]"
        else:
            record.exchange = "[UNKNOWN]"
        return super().format(record)

# Configure logging with exchange prefix
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(exchange)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Set up the custom formatter
for handler in logger.handlers:
    handler.setFormatter(ExchangeFormatter())

# If no handlers exist, add one
if not logger.handlers:
    handler = logging.StreamHandler()
    handler.setFormatter(ExchangeFormatter())
    logger.addHandler(handler)

# Configuration
MONGO_URL = os.getenv("MONGO_SERVER", "mongodb://localhost:27017")
DB_NAME = "tso"
COLLECTION_NAME = "symbols"
general_source_id = 'ccxtPy'

# List of tokens to track (base currencies)
TOKENS = [
    "BTC", "ETH", "BNB", "XRP", "ADA", "SOL", "DOGE", "DOT", "AVAX", "SHIB",
    "MATIC", "LTC", "UNI", "LINK", "ATOM", "ETC", "XLM", "BCH", "ALGO", "VET",
    "ICP", "FIL", "TRX", "APE", "NEAR", "MANA", "SAND", "AXS", "THETA", "AAVE"
]

# List of quote currencies to pair with tokens
QUOTE_CURRENCIES = ["USD", "USDT", "USDC"]



class PriceTracker:
    def __init__(self, exchange_id: str):
        self.exchange_id = exchange_id
        self.price_data = defaultdict(lambda: defaultdict(dict))  # {token: {exchange: {pair: price}}}
        self.exchange = None
        self.client = None  # MongoDB client, initialize as None
        self.price_collections = {}

        self.shutdown_event = asyncio.Event()
        self.tasks = []
        self.lock = asyncio.Lock()
        self.trackedStuff = []
        
    def current_timestamp(self):
        """Returns the current UTC timestamp in whole seconds."""
        return round(datetime.utcnow().timestamp())

    async def initialize(self):
        """Initialize MongoDB connection and exchange instance."""
        logger.info(f"Initializing Price Tracker...")
        
        # Initialize MongoDB
        try:
            self.client = MongoClient(MONGO_URL)
            # Test connection
            self.client.admin.command('ping')
            logger.info("✅ MongoDB connection established")
        except Exception as e:
            logger.error(f"❌ Failed to connect to MongoDB: {e}")
            raise
        
        # Initialize single exchange
        try:
            exchange_class = getattr(ccxt, self.exchange_id)
            self.exchange = exchange_class({
                'sandbox': False,
                'enableRateLimit': True,
            })
            logger.info(f"✅ Successfully initialized exchange")
            return True
        except Exception as e:
            logger.error(f"❌ Failed to initialize exchange: {e}")
            return False
    
    async def get_available_pairs(self) -> List[str]:
        """Get available trading pairs for the exchange."""
        try:
            await self.exchange.load_markets()
            available_pairs = []
            
            for token in TOKENS:
                for quote in QUOTE_CURRENCIES:
                    if self.exchange_id == "htx" and quote == "USDC":
                        pass

                    symbol = f"{token}/{quote}"
                    if symbol in self.exchange.markets:
                        available_pairs.append({"base": token, "quote": quote, "market": symbol})
            
            logger.info(f"📊 Found {len(available_pairs)} available pairs")
            return available_pairs
            
        except Exception as e:
            logger.error(f"❌ Error loading markets: {e}")
            return []
    
    async def watch_ticker(self, symbol: str):
        """Watch ticker for a specific symbol on the exchange - runs once to establish the watch."""
        tickerId = f"{self.exchange_id}_{symbol}_ticker_py"
        try:
            await self.exchange.watch_ticker(symbol)
            self.trackedStuff.append({"exchange": self.exchange, "method":"ticker", "symbol":symbol, "source": tickerId})

        except Exception as e:
            logger.error(f"❌ Error establishing ticker watch for {symbol}: {e}")

    async def watch_orderbook(self, symbol: str):
        """Watch orderbook for a specific symbol on the exchange - runs once to establish the watch."""
        try:
            if not self.exchange.has.get('watchOrderBook'):
                return
            
            default_limit = 20
            limit_overrides = {
                "bitfinex": 25, "bitfinex1": 25, "bitmex": 25,
                "bybit": 1, "kraken": 10, "poloniexfutures": 5
            }
            limit = limit_overrides.get(self.exchange_id, default_limit) 

            orderbookId = f"{self.exchange_id}_{symbol}_orderbook_py"
            
            await self.exchange.watch_order_book(symbol, limit)
            self.trackedStuff.append({"exchange": self.exchange, "method": "orderbook", "symbol":symbol, "source": orderbookId})

        except Exception as e:
            logger.error(f"❌ Error establishing orderbook watch for {symbol}: {e}")
    
    async def start_exchange_watchers(self):
        """Start all watchers for the exchange."""
        pairs = await self.get_available_pairs()
        if not pairs:
            logger.warning(f"⚠️ No pairs available")
            return
        
        exchange_tasks = []
        for pair in pairs:
            symbol = pair['market']
            # Create ticker watcher
            if self.exchange.has.get('watchTicker'):
                task = asyncio.create_task(self.watch_ticker(symbol))
                exchange_tasks.append(task)
            
            # Create orderbook watcher
            if self.exchange.has.get('watchOrderBook'):
                task = asyncio.create_task(self.watch_orderbook(symbol))
                exchange_tasks.append(task)
        
        logger.info(f"👀 Started {len(exchange_tasks)} watchers")
        self.tasks.extend(exchange_tasks)
        
        try:
            await asyncio.gather(*exchange_tasks, return_exceptions=True)
        except Exception as e:
            logger.error(f"❌ Error in exchange watchers: {e}")
    
    def get_price_collection(self, symbol):
        if not self.client:  # Initialize MongoDB client if not already connected
            self.client = MongoClient(MONGO_URL)
        
        db = self.client[DB_NAME]

        if symbol not in self.price_collections:
            self.price_collections[symbol] = db[f"prices-{symbol}"]

        return self.price_collections[symbol]

    async def save_to_mongo_minimal_test(self):
        """Minimal version to test timing precision."""
        logger.info("💾 Started MINIMAL MongoDB save task")
        
        loop_counter = 0
        start_time = time.time()
        
        while not self.shutdown_event.is_set():
            loop_start = time.time()
            loop_counter += 1
            
            # Absolute minimum work
            tracked_count = len(self.trackedStuff)
            current_ts = self.current_timestamp()
            
            # Just print timing info
            elapsed_since_start = loop_start - start_time
            expected_elapsed = loop_counter * 1.0
            drift = elapsed_since_start - expected_elapsed
            
            print(f"Loop #{loop_counter}: {current_ts}, tracked={tracked_count}, drift={drift:.3f}s")
            
            # Precise 1-second timing
            next_target = start_time + (loop_counter * 1.0)
            sleep_time = next_target - time.time()
            
            if sleep_time > 0:
                await asyncio.sleep(sleep_time)
            else:
                await asyncio.sleep(0.001)  # Brief yield
    
    async def run(self):
        """Main run method."""
        if not await self.initialize():
            logger.error("❌ Failed to initialize. Exiting.")
            return
        
        try:
            # Start exchange watchers
            watcher_task = asyncio.create_task(self.start_exchange_watchers())
            
            # Start MongoDB save task
            save_task = asyncio.create_task(self.save_to_mongo_minimal_test())
            
            # Combine all tasks
            all_tasks = [watcher_task, save_task]
            self.tasks.extend(all_tasks)
            
            logger.info(f"🚀 All systems running! Waiting for termination signal...")
            
            # Wait for all tasks
            await asyncio.gather(*all_tasks, return_exceptions=True)
            
        except KeyboardInterrupt:
            logger.info("\n⚠️ Received interrupt signal")
        except Exception as e:
            logger.error(f"❌ Unexpected error: {e}")
        finally:
            await self.cleanup()
    
    async def cleanup(self):
        """Clean up all resources."""
        logger.info("🧹 Starting cleanup...")
        
        # Signal shutdown
        self.shutdown_event.set()
        
        # Cancel all tasks
        if self.tasks:
            logger.info(f"🛑 Cancelling {len(self.tasks)} tasks...")
            for task in self.tasks:
                if not task.done():
                    task.cancel()
            
            # Wait for tasks to finish cancelling
            await asyncio.gather(*self.tasks, return_exceptions=True)
        
        # Close exchange
        if self.exchange:
            logger.info(f"🏦 Closing exchange...")
            try:
                await self.exchange.close()
            except Exception as e:
                logger.error(f"❌ Error closing exchange: {e}")
        
        # Close MongoDB connection
        if self.client:
            logger.info("🔌 Closing MongoDB connection...")
            self.client.close()
        
        logger.info("✅ Cleanup completed")

# Global tracker instance for signal handling
tracker = None

def signal_handler(signum, frame):
    """Handle interrupt signals."""
    global tracker
    logger.info(f"\n⚠️ Received signal {signum}")
    if tracker and not tracker.shutdown_event.is_set():
        # Create a new event loop if none exists
        try:
            loop = asyncio.get_event_loop()
        except RuntimeError:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
        
        # Schedule cleanup
        if loop.is_running():
            loop.create_task(tracker.cleanup())
        else:
            loop.run_until_complete(tracker.cleanup())

async def main():
    """Main function."""
    global tracker, CURRENT_EXCHANGE_ID
    
    # Check command line arguments
    if len(sys.argv) != 2:
        logger.error("❌ Usage: python script.py <exchange_name>")
        sys.exit(1)
    
    exchange_id = sys.argv[1]
    CURRENT_EXCHANGE_ID = exchange_id  # Set global exchange ID for logging
    logger.info(f"🎯 Starting price tracker...")
    
    # Set up signal handlers
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    # Create and run tracker
    tracker = PriceTracker(exchange_id)
    
    try:
        await tracker.run()
    except KeyboardInterrupt:
        logger.info("\n❌ Script interrupted by user")
    except Exception as e:
        logger.error(f"🚨 Fatal error: {e}")
    finally:
        logger.info("🛑 Script terminated")

if __name__ == "__main__":
    asyncio.run(main())