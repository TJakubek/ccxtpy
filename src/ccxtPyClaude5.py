import asyncio
import signal
import time
import sys
from datetime import datetime
from collections import defaultdict
from typing import Dict, List, Set
import os
from dotenv import load_dotenv
import atexit

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

# Configuration
MONGO_URL = os.getenv("MONGO_SERVER", "mongodb://tsoData:naefeed0@65.109.52.201:27017")
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
        # Changed to dict to ensure uniqueness by method+symbol combination
        self.trackedStuff = {}  # Key: (method, symbol), Value: item dict
        self._cleanup_done = False
        
        # Register cleanup on exit
        atexit.register(self.sync_cleanup)
        
    def current_timestamp(self):
        """Returns the current UTC timestamp in whole seconds."""
        return round(datetime.now().timestamp())

    async def initialize(self):
        """Initialize MongoDB connection and exchange instance."""
        logger.info(f"Initializing Price Tracker for exchange: {self.exchange_id}...")
        
        # Initialize MongoDB
        try:
            self.client = MongoClient(
                MONGO_URL,
                serverSelectionTimeoutMS=5000,  # 5 second timeout
                connectTimeoutMS=5000,
                socketTimeoutMS=5000
            )
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
            logger.info(f"✅ Successfully initialized exchange: {self.exchange_id}")
            return True
        except Exception as e:
            logger.error(f"❌ Failed to initialize exchange '{self.exchange_id}': {e}")
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
            
            logger.info(f"📊 Exchange '{self.exchange_id}': Found {len(available_pairs)} available pairs")
            return available_pairs
            
        except Exception as e:
            logger.error(f"❌ Error loading markets for exchange '{self.exchange_id}': {e}")
            return []
    
    def _add_tracked_item(self, method: str, symbol: str, exchange, source: str):
        """Add or update a tracked item, ensuring uniqueness by method+symbol."""
        key = (method, symbol)
        item = {
            "exchange": exchange,
            "method": method,
            "symbol": symbol,
            "source": source,
            "timestamp": time.time()  # Track when this item was added
        }
        
        # If key already exists, we're replacing the old item with the new one
        if key in self.trackedStuff:
            logger.debug(f"🔄 Replacing existing {method} watch for {symbol}")
        else:
            logger.debug(f"➕ Adding new {method} watch for {symbol}")
            
        self.trackedStuff[key] = item
    
    async def watch_ticker(self, symbol: str):
        """Watch ticker for a specific symbol on the exchange - runs once to establish the watch."""
        tickerId = f"{self.exchange_id}_{symbol}_ticker_py"
        try:
            await self.exchange.watch_ticker(symbol)
            self._add_tracked_item("ticker", symbol, self.exchange, tickerId)

        except Exception as e:
            logger.error(f"❌ Error establishing ticker watch for '{self.exchange_id}' {symbol}: {e}")

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
            self._add_tracked_item("orderbook", symbol, self.exchange, orderbookId)

        except Exception as e:
            logger.error(f"❌ Error establishing orderbook watch for '{self.exchange_id}' {symbol}: {e}")
    
    async def start_exchange_watchers(self):
        """Start all watchers for the exchange."""
        pairs = await self.get_available_pairs()
        if not pairs:
            logger.warning(f"⚠️ No pairs available for exchange '{self.exchange_id}'")
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
        
        logger.info(f"👀 Started {len(exchange_tasks)} watchers for exchange '{self.exchange_id}'")
        self.tasks.extend(exchange_tasks)
        
        try:
            await asyncio.gather(*exchange_tasks, return_exceptions=True)
        except Exception as e:
            logger.error(f"❌ Error in exchange watchers for '{self.exchange_id}': {e}")
    
    def get_price_collection(self, symbol):
        if not self.client:  # Initialize MongoDB client if not already connected
            self.client = MongoClient(
                MONGO_URL,
                serverSelectionTimeoutMS=5000,
                connectTimeoutMS=5000,
                socketTimeoutMS=5000
            )
        
        db = self.client[DB_NAME]

        if symbol not in self.price_collections:
            self.price_collections[symbol] = db[f"prices-{symbol}"]

        return self.price_collections[symbol]

    async def save_to_mongo(self):
        """Save current price data to MongoDB every second using bulk operations."""
        logger.info("💾 Started MongoDB save task")
        
        while not self.shutdown_event.is_set():
            try:
                start_time = time.time()
                current_ts = self.current_timestamp()
                
                # Collect price data from all tracked items (now iterating over dict values)
                priceDict = {}

                for item in self.trackedStuff.values():  # Changed from list to dict.values()
                    exchange = item.get("exchange")
                    method = item.get("method")
                    symbol = item.get("symbol")
                    source = item.get("source")
                    token = symbol.split('/')[0]
                    
                    if method == "ticker":
                        ticker = exchange.tickers.get(symbol)
                        if not ticker:
                            continue
                            
                        if "last" in ticker and ticker["last"] is not None:
                            price = float(ticker["last"])
                            if token not in priceDict:
                                priceDict[token] = []
                            priceDict[token].append({"sourceId": f"{source}_last", "price": price})

                        if "average" in ticker and ticker["average"] is not None:
                            price = float(ticker["average"])
                            if token not in priceDict:
                                priceDict[token] = []
                            priceDict[token].append({"sourceId": f"{source}_average", "price": price})

                    elif method == "orderbook":
                        orderbook = exchange.orderbooks.get(symbol)
                        if orderbook and orderbook.get('bids') and orderbook.get('asks'):
                            bid = orderbook['bids'][0][0] if orderbook['bids'] else None
                            ask = orderbook['asks'][0][0] if orderbook['asks'] else None
                            
                            if bid and ask:
                                mid_price = (bid + ask) / 2
                                if token not in priceDict:
                                    priceDict[token] = []
                                priceDict[token].append({"sourceId": source, "price": mid_price})

                # Save to MongoDB for each token
                total_prices_saved = 0
                for token, prices_data in priceDict.items():
                    if self.shutdown_event.is_set():  # Check shutdown during save
                        break
                        
                    collection = self.get_price_collection(token)
                    
                    # First try to find existing document
                    existing_doc = collection.find_one({'timestamp': current_ts, 'sourceId': general_source_id})
                    
                    if existing_doc:
                        # Document exists, append new prices to existing array
                        result = collection.update_one(
                            {'timestamp': current_ts, 'sourceId': general_source_id},
                            {
                                '$addToSet': {
                                    'prices': {'$each': prices_data}
                                }
                            }
                        )
                    else:
                        # Document doesn't exist, create new one
                        result = collection.update_one(
                            {'timestamp': current_ts, 'sourceId': general_source_id},
                            {
                                '$setOnInsert': {
                                    'timestamp': current_ts,
                                    'sourceId': general_source_id,
                                    'prices': prices_data
                                }
                            },
                            upsert=True
                        )
                    
                    total_prices_saved += len(prices_data)

                save_time = time.time() - start_time
                logger.info(f"{current_ts} saved {total_prices_saved} prices from {len(self.trackedStuff)} unique watchers in {save_time:.3f}s")
                
                # Sleep for the remainder of the second
                elapsed = time.time() - start_time
                sleep_time = max(0, 1.0 - elapsed)
                
                if sleep_time > 0.05:
                    await asyncio.sleep(sleep_time)
                else:
                    logger.warning(f"⚠️ Save operation took {elapsed:.3f}s (longer than 1s interval)")
                    
            except Exception as e:
                logger.error(f"❌ Error saving to MongoDB: {e}")
                if not self.shutdown_event.is_set():
                    await asyncio.sleep(1)
    
    async def run(self):
        """Main run method."""
        if not await self.initialize():
            logger.error("❌ Failed to initialize. Exiting.")
            return
        
        try:
            # Start exchange watchers
            watcher_task = asyncio.create_task(self.start_exchange_watchers())
            
            # Start MongoDB save task
            save_task = asyncio.create_task(self.save_to_mongo())
            
            # Combine all tasks
            all_tasks = [watcher_task, save_task]
            self.tasks.extend(all_tasks)
            
            logger.info(f"🚀 All systems running for exchange '{self.exchange_id}'! Waiting for termination signal...")
            
            # Wait for all tasks
            await asyncio.gather(*all_tasks, return_exceptions=True)
            
        except KeyboardInterrupt:
            logger.info("\n⚠️ Received interrupt signal")
        except Exception as e:
            logger.error(f"❌ Unexpected error: {e}")
        finally:
            await self.cleanup()
    
    def sync_cleanup(self):
        """Synchronous cleanup for atexit handler."""
        if self._cleanup_done:
            return
            
        logger.info("🧹 Performing synchronous cleanup...")
        
        # Close MongoDB connection
        if self.client:
            try:
                self.client.close()
                logger.info("🔌 MongoDB connection closed (sync)")
            except Exception as e:
                logger.error(f"❌ Error closing MongoDB connection (sync): {e}")
        
        self._cleanup_done = True
    
    async def cleanup(self):
        """Clean up all resources."""
        if self._cleanup_done:
            return
            
        logger.info("🧹 Starting async cleanup...")
        
        # Signal shutdown
        self.shutdown_event.set()
        
        # Cancel all tasks
        if self.tasks:
            logger.info(f"🛑 Cancelling {len(self.tasks)} tasks...")
            for task in self.tasks:
                if not task.done():
                    task.cancel()
            
            # Wait for tasks to finish cancelling with timeout
            try:
                await asyncio.wait_for(
                    asyncio.gather(*self.tasks, return_exceptions=True),
                    timeout=5.0
                )
            except asyncio.TimeoutError:
                logger.warning("⚠️ Task cancellation timed out")
        
        # Close exchange
        if self.exchange:
            logger.info(f"🏦 Closing exchange '{self.exchange_id}'...")
            try:
                await self.exchange.close()
            except Exception as e:
                logger.error(f"❌ Error closing exchange '{self.exchange_id}': {e}")
        
        # Close MongoDB connection
        if self.client:
            logger.info("🔌 Closing MongoDB connection...")
            try:
                self.client.close()
            except Exception as e:
                logger.error(f"❌ Error closing MongoDB connection: {e}")
        
        self._cleanup_done = True
        logger.info("✅ Cleanup completed")

# Global tracker instance for signal handling
tracker = None

def signal_handler(signum, frame):
    """Handle interrupt signals."""
    global tracker
    logger.info(f"\n⚠️ Received signal {signum}")
    if tracker and not tracker.shutdown_event.is_set():
        tracker.shutdown_event.set()
        
        # Force immediate cleanup if possible
        try:
            loop = asyncio.get_running_loop()
            loop.create_task(tracker.cleanup())
        except RuntimeError:
            # No running loop, do sync cleanup
            tracker.sync_cleanup()

async def main():
    """Main function."""
    global tracker
    
    # Check command line arguments
    if len(sys.argv) != 2:
        logger.error("❌ Usage: python script.py <exchange_name>")
        sys.exit(1)
    
    exchange_id = sys.argv[1]
    logger.info(f"🎯 Starting price tracker for exchange: '{exchange_id}'")
    
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
        if tracker:
            await tracker.cleanup()
        logger.info("🛑 Script terminated")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("🛑 Final cleanup on KeyboardInterrupt")
    finally:
        # Force cleanup if tracker exists
        if tracker:
            tracker.sync_cleanup()