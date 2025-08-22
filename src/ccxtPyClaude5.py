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

# Default tokens to track (base currencies) - used as fallback
DEFAULT_TOKENS = [
    "BTC", "ETH", "BNB", "XRP", "ADA", "SOL", "DOGE", "DOT", "AVAX", "SHIB",
    "MATIC", "LTC", "UNI", "LINK", "ATOM", "ETC", "XLM", "BCH", "ALGO", "VET",
    "ICP", "FIL", "TRX", "APE", "NEAR", "MANA", "SAND", "AXS", "THETA", "AAVE"
]

# List of quote currencies to pair with tokens
QUOTE_CURRENCIES = ["USD", "USDT", "USDC", "USDT:USDT"]

class PriceTracker:
    def __init__(self, exchange_id: str, tokens: List[str] = None):
        self.exchange_id = exchange_id
        self.tokens = tokens if tokens else DEFAULT_TOKENS
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
        
        # Health monitoring
        self.last_save_time = time.time()
        self.last_successful_save_time = time.time()  # Track when we actually saved prices
        self.last_data_received = time.time()
        self.health_check_interval = 30  # Check every 30 seconds
        self.max_save_silence = 300  # Alert if no saves for 5 minutes
        self.max_data_silence = 60   # Alert if no data for 1 minute
        self.max_no_prices_restart = 300  # Restart if no prices saved for 5 minutes
        
        # Register cleanup on exit
        atexit.register(self.sync_cleanup)
        
    def current_timestamp(self):
        """Returns the current UTC timestamp in whole seconds."""
        return round(datetime.now().timestamp())
    
    async def health_monitor(self):
        """Monitor system health and report issues."""
        logger.info(f"🏥 Started health monitor for '{self.exchange_id}'")
        
        while not self.shutdown_event.is_set():
            try:
                current_time = time.time()
                
                # Check if saves are happening
                time_since_save = current_time - self.last_save_time
                if time_since_save > self.max_save_silence:
                    logger.warning(f"⚠️ [{self.exchange_id}] No MongoDB saves for {time_since_save:.0f}s - possible issue")
                
                # Check if data is being received
                time_since_data = current_time - self.last_data_received
                if time_since_data > self.max_data_silence:
                    logger.warning(f"⚠️ [{self.exchange_id}] No price data received for {time_since_data:.0f}s - possible connection issue")
                
                # Check if we need to restart due to no prices being saved
                time_since_successful_save = current_time - self.last_successful_save_time
                if time_since_successful_save > self.max_no_prices_restart:
                    logger.error(f"🚨 [{self.exchange_id}] No prices saved for {time_since_successful_save:.0f}s - triggering restart")
                    # Trigger restart by setting shutdown event and exiting with non-zero code
                    self.shutdown_event.set()
                    # Exit with code 2 to signal restart needed
                    import os
                    os._exit(2)
                
                # Check MongoDB connection
                if self.client:
                    try:
                        self.client.admin.command('ping')
                    except Exception as e:
                        logger.error(f"❌ [{self.exchange_id}] MongoDB connection failed: {e}")
                
                # Check exchange connection
                if self.exchange and hasattr(self.exchange, 'ping') and callable(getattr(self.exchange, 'ping', None)):
                    try:
                        await self.exchange.ping()
                    except Exception as e:
                        logger.warning(f"⚠️ [{self.exchange_id}] Exchange ping failed: {e}")
                
                # Report health stats
                active_watchers = len(self.trackedStuff)
                logger.info(f"💓 [{self.exchange_id}] Health: {active_watchers} watchers, last save {time_since_save:.0f}s ago, last data {time_since_data:.0f}s ago, last prices {time_since_successful_save:.0f}s ago")
                
                await asyncio.sleep(self.health_check_interval)
                
            except Exception as e:
                logger.error(f"❌ [{self.exchange_id}] Health monitor error: {e}")
                await asyncio.sleep(self.health_check_interval)

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

            # # log markets
            # logger.info(f"📈 [{self.exchange_id}] Available markets: {list(self.exchange.markets.keys())}")

            for token in self.tokens:
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
        """Start all watchers for the exchange using bulk methods when possible."""
        pairs = await self.get_available_pairs()
        if not pairs:
            logger.warning(f"⚠️ No pairs available for exchange '{self.exchange_id}'")
            return
        
        symbols = [pair['market'] for pair in pairs]
        logger.info(f"👀 Setting up watchers for {len(symbols)} symbols on '{self.exchange_id}'")
        
        try:
            # Try bulk methods first (more efficient)
            if self.exchange_id != 'bybit' and self.exchange_id != 'bitmex' and self.exchange_id != 'gate' and self.exchange_id != 'gateio' and await self._setup_bulk_watchers(symbols):
                logger.info(f"✅ Bulk watchers set up for '{self.exchange_id}'")
            else:
                # Fallback to individual watchers
                await self._setup_individual_watchers(symbols)
                logger.info(f"✅ Individual watchers set up for '{self.exchange_id}'")
            
            # Keep the watcher task alive by waiting for shutdown
            while not self.shutdown_event.is_set():
                await asyncio.sleep(5)  # Check every 5 seconds
                
        except Exception as e:
            logger.error(f"❌ Error in exchange watchers for '{self.exchange_id}': {e}")
            
        logger.info(f"🏁 Exchange watchers shutting down for '{self.exchange_id}'")
    
    async def _setup_bulk_watchers(self, symbols: list) -> bool:
        """Try to set up bulk watchers. Returns True if successful, False otherwise."""
        bulk_success = False
        
        # Try bulk ticker watching
        if self.exchange.has.get('watchTickers'):
            try:
                logger.info(f"🔄 [{self.exchange_id}] Using bulk ticker watching for {len(symbols)} symbols")
                await self.exchange.watch_tickers(symbols)
                
                # Track all symbols as ticker watchers
                for symbol in symbols:
                    tickerId = f"{self.exchange_id}_{symbol}_ticker_py"
                    self._add_tracked_item("ticker", symbol, self.exchange, tickerId)
                
                bulk_success = True
                logger.info(f"✅ [{self.exchange_id}] Bulk ticker watching established")
            except Exception as e:
                logger.warning(f"⚠️ [{self.exchange_id}] Bulk ticker watching failed: {e}")
        
        # Try bulk orderbook watching (newer CCXT versions)
        if self.exchange.has.get('watchOrderBookForSymbols'):
            try:
                default_limit = 20
                limit_overrides = {
                    "bitfinex": 25, "bitfinex1": 25, "bitmex": 25,
                    "bybit": 1, "kraken": 10, "poloniexfutures": 5
                }
                limit = limit_overrides.get(self.exchange_id, default_limit)
                
                logger.info(f"🔄 [{self.exchange_id}] Using bulk orderbook watching for {len(symbols)} symbols")
                await self.exchange.watch_order_book_for_symbols(symbols, limit)
                
                # Track all symbols as orderbook watchers
                for symbol in symbols:
                    orderbookId = f"{self.exchange_id}_{symbol}_orderbook_py"
                    self._add_tracked_item("orderbook", symbol, self.exchange, orderbookId)
                
                bulk_success = True
                logger.info(f"✅ [{self.exchange_id}] Bulk orderbook watching established")
            except Exception as e:
                logger.warning(f"⚠️ [{self.exchange_id}] Bulk orderbook watching failed: {e}")
        
        return bulk_success
    
    async def _setup_individual_watchers(self, symbols: list):
        """Set up individual watchers as fallback."""
        logger.info(f"🔄 [{self.exchange_id}] Using individual watchers for {len(symbols)} symbols")
        
        # Batch individual watchers to avoid overwhelming the exchange
        batch_size = 10  # Process 10 symbols at a time
        for i in range(0, len(symbols), batch_size):
            batch = symbols[i:i + batch_size]
            setup_tasks = []
            
            for symbol in batch:
                # Create ticker watcher
                if self.exchange.has.get('watchTicker'):
                    task = asyncio.create_task(self.watch_ticker(symbol))
                    setup_tasks.append(task)
                
                # Create orderbook watcher
                if self.exchange.has.get('watchOrderBook'):
                    task = asyncio.create_task(self.watch_orderbook(symbol))
                    setup_tasks.append(task)
            
            # Wait for batch to complete
            if setup_tasks:
                results = await asyncio.gather(*setup_tasks, return_exceptions=True)
                
                # Check for exceptions in this batch
                for j, result in enumerate(results):
                    if isinstance(result, Exception):
                        logger.error(f"❌ [{self.exchange_id}] Watcher setup failed for batch {i//batch_size + 1}, task {j}: {result}")
                
                # Small delay between batches to respect rate limits
                if i + batch_size < len(symbols):
                    await asyncio.sleep(1)
    
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
                data_received = False

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
                            data_received = True

                        if "average" in ticker and ticker["average"] is not None:
                            price = float(ticker["average"])
                            if token not in priceDict:
                                priceDict[token] = []
                            priceDict[token].append({"sourceId": f"{source}_average", "price": price})
                            data_received = True

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
                                data_received = True
                
                # Update last data received time if we got any data
                if data_received:
                    self.last_data_received = time.time()

                # Save to MongoDB for each token (synchronous operations)
                total_prices_saved = 0
                save_start = time.time()
                
                for token, prices_data in priceDict.items():
                    if self.shutdown_event.is_set():  # Check shutdown during save
                        break
                        
                    # Check if we're approaching 0.8s limit for saves
                    if time.time() - save_start > 0.8:
                        logger.warning(f"⚠️ Skipping remaining MongoDB saves (time limit reached)")
                        break
                        
                    try:
                        collection = self.get_price_collection(token)
                        
                        # Single upsert operation - create document with prices or add to existing
                        collection.update_one(
                            {'timestamp': current_ts, 'sourceId': general_source_id},
                            {
                                '$setOnInsert': {
                                    'timestamp': current_ts,
                                    'sourceId': general_source_id
                                },
                                '$addToSet': {
                                    'prices': {'$each': prices_data}
                                }
                            },
                            upsert=True
                        )
                        
                        total_prices_saved += len(prices_data)
                        
                    except Exception as e:
                        logger.error(f"❌ Error saving {token} prices: {e}")
                        continue

                save_time = time.time() - start_time
                self.last_save_time = time.time()  # Update last save time for health monitoring
                
                # Track successful saves (only when prices were actually saved)
                if total_prices_saved > 0:
                    self.last_successful_save_time = time.time()
                
                logger.info(f"{current_ts} │ {self.exchange_id:12} │ {total_prices_saved:4} prices │ {len(self.trackedStuff):3} watchers │ {save_time:.3f}s")
                
                # Calculate sleep time to maintain <= 1s total cycle time
                elapsed = time.time() - start_time
                
                # Cap maximum cycle time at 1 second
                if elapsed >= 1.0:
                    logger.warning(f"⚠️ Save operation took {elapsed:.3f}s (>= 1s), skipping sleep")
                    # Don't sleep if we're already at or over 1 second
                    continue
                else:
                    # Sleep for remainder of 1 second, but cap at 0.95s to prevent drift
                    sleep_time = min(1.0 - elapsed, 0.95)
                    if sleep_time > 0.01:  # Only sleep if meaningful time remaining
                        await asyncio.sleep(sleep_time)
                    
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
            
            # Start health monitor
            health_task = asyncio.create_task(self.health_monitor())
            
            # Combine all tasks
            all_tasks = [watcher_task, save_task, health_task]
            self.tasks.extend(all_tasks)
            
            logger.info(f"🚀 All systems running for exchange '{self.exchange_id}'! Waiting for termination signal...")
            
            # Monitor tasks and shutdown event
            while not self.shutdown_event.is_set():
                # Check if any critical task has finished unexpectedly
                done_tasks = [task for task in all_tasks if task.done()]
                if done_tasks:
                    logger.warning(f"⚠️ {len(done_tasks)} tasks completed unexpectedly")
                    task_names = ["watcher_task", "save_task", "health_task"]
                    for i, task in enumerate(done_tasks):
                        task_name = task_names[i] if i < len(task_names) else f"task_{i}"
                        try:
                            exception = task.exception()
                            if exception:
                                logger.error(f"❌ {task_name} failed with: {exception}")
                                import traceback
                                logger.error(f"❌ {task_name} traceback: {traceback.format_exception(type(exception), exception, exception.__traceback__)}")
                            else:
                                logger.warning(f"⚠️ {task_name} completed normally (unexpected)")
                        except asyncio.InvalidStateError:
                            logger.warning(f"⚠️ {task_name} state unavailable")
                    break
                
                # Wait a bit before checking again, or until shutdown signal
                try:
                    await asyncio.wait_for(self.shutdown_event.wait(), timeout=1.0)
                except asyncio.TimeoutError:
                    pass
            
            logger.info(f"🛑 [{self.exchange_id}] Shutdown initiated, cleaning up...")
            
        except KeyboardInterrupt:
            logger.info(f"\n⚠️ [{self.exchange_id}] Received interrupt signal")
        except Exception as e:
            logger.error(f"❌ [{self.exchange_id}] Unexpected error: {e}")
        finally:
            await self.cleanup()
    
    def sync_cleanup(self):
        """Synchronous cleanup for atexit handler."""
        if self._cleanup_done:
            return
            
        logger.info(f"🧹 [{self.exchange_id}] Performing synchronous cleanup...")
        
        # Close MongoDB connection
        if self.client:
            try:
                self.client.close()
                logger.info(f"🔌 [{self.exchange_id}] MongoDB connection closed (sync)")
            except Exception as e:
                logger.error(f"❌ [{self.exchange_id}] Error closing MongoDB connection (sync): {e}")
        
        self._cleanup_done = True
    
    async def cleanup(self):
        """Clean up all resources."""
        if self._cleanup_done:
            return
            
        logger.info(f"🧹 [{self.exchange_id}] Starting async cleanup...")
        
        # Signal shutdown first
        self.shutdown_event.set()
        
        # Close exchange connections first to stop new data
        if self.exchange:
            logger.info(f"🏦 [{self.exchange_id}] Closing exchange...")
            try:
                # Give exchange 3 seconds to close gracefully
                await asyncio.wait_for(self.exchange.close(), timeout=3.0)
                logger.info(f"✅ [{self.exchange_id}] Exchange closed successfully")
            except asyncio.TimeoutError:
                logger.warning(f"⚠️ [{self.exchange_id}] Exchange close timed out")
            except Exception as e:
                logger.error(f"❌ [{self.exchange_id}] Error closing exchange: {e}")
        
        # Cancel all tasks after exchange is closed
        if self.tasks:
            logger.info(f"🛑 [{self.exchange_id}] Cancelling {len(self.tasks)} tasks...")
            for task in self.tasks:
                if not task.done():
                    task.cancel()
            
            # Wait for tasks to finish cancelling with timeout
            try:
                await asyncio.wait_for(
                    asyncio.gather(*self.tasks, return_exceptions=True),
                    timeout=3.0
                )
                logger.info(f"✅ [{self.exchange_id}] All tasks cancelled successfully")
            except asyncio.TimeoutError:
                logger.warning(f"⚠️ [{self.exchange_id}] Task cancellation timed out after 3s")
        
        # Final MongoDB save before closing
        if self.client and self.trackedStuff:
            logger.info(f"💾 [{self.exchange_id}] Performing final MongoDB save...")
            try:
                # Quick final save of current data
                current_ts = self.current_timestamp()
                priceDict = {}
                
                for item in self.trackedStuff.values():
                    exchange = item.get("exchange")
                    method = item.get("method")
                    symbol = item.get("symbol")
                    source = item.get("source")
                    token = symbol.split('/')[0]
                    
                    if method == "ticker":
                        ticker = exchange.tickers.get(symbol)
                        if ticker and "last" in ticker and ticker["last"] is not None:
                            price = float(ticker["last"])
                            if token not in priceDict:
                                priceDict[token] = []
                            priceDict[token].append({"sourceId": f"{source}_last", "price": price})
                
                # Save final data
                for token, prices_data in priceDict.items():
                    collection = self.get_price_collection(token)
                    collection.update_one(
                        {'timestamp': current_ts, 'sourceId': general_source_id},
                        {'$setOnInsert': {'timestamp': current_ts, 'sourceId': general_source_id, 'prices': prices_data}},
                        upsert=True
                    )
                
                logger.info(f"✅ [{self.exchange_id}] Final save completed: {sum(len(p) for p in priceDict.values())} prices")
            except Exception as e:
                logger.error(f"❌ [{self.exchange_id}] Error in final save: {e}")
        
        # Close MongoDB connection
        if self.client:
            logger.info(f"🔌 [{self.exchange_id}] Closing MongoDB connection...")
            try:
                self.client.close()
                logger.info(f"✅ [{self.exchange_id}] MongoDB connection closed")
            except Exception as e:
                logger.error(f"❌ [{self.exchange_id}] Error closing MongoDB connection: {e}")
        
        self._cleanup_done = True
        logger.info(f"✅ [{self.exchange_id}] Cleanup completed")

# Global tracker instance for signal handling
tracker = None

def signal_handler(signum, frame):
    """Handle interrupt signals."""
    global tracker
    logger.info(f"\n⚠️ Received signal {signum} - initiating graceful shutdown")
    if tracker and not tracker.shutdown_event.is_set():
        tracker.shutdown_event.set()
        
        # Schedule cleanup task
        try:
            loop = asyncio.get_running_loop()
            # Don't create new task, let the main loop handle shutdown
            logger.info("🛑 Shutdown event set, main loop will handle cleanup")
        except RuntimeError:
            # No running loop, do sync cleanup as last resort
            logger.warning("⚠️ No event loop found, performing sync cleanup")
            tracker.sync_cleanup()

async def main():
    """Main function."""
    global tracker
    
    # Check command line arguments
    if len(sys.argv) < 2:
        logger.error("❌ Usage: python script.py <exchange_name> [tokens_comma_separated]")
        sys.exit(1)
    
    exchange_id = sys.argv[1]
    
    # Parse tokens if provided
    tokens = None
    if len(sys.argv) > 2:
        tokens_str = sys.argv[2]
        tokens = [token.strip() for token in tokens_str.split(",") if token.strip()]
        logger.info(f"🎯 Starting price tracker for exchange: '{exchange_id}' with {len(tokens)} tokens from MongoDB")
    else:
        logger.info(f"🎯 Starting price tracker for exchange: '{exchange_id}' with default tokens")
    
    # Set up signal handlers
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    # Create and run tracker
    tracker = PriceTracker(exchange_id, tokens)
    
    try:
        await tracker.run()
    except KeyboardInterrupt:
        logger.info(f"\n❌ [{exchange_id}] Script interrupted by user")
    except Exception as e:
        logger.error(f"🚨 [{exchange_id}] Fatal error: {e}")
    finally:
        if tracker:
            await tracker.cleanup()
        logger.info(f"🛑 [{exchange_id}] Script terminated")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("🛑 Final cleanup on KeyboardInterrupt")
    finally:
        # Force cleanup if tracker exists
        if tracker:
            tracker.sync_cleanup()