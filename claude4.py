import asyncio
import signal
import time
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

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

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

# List of exchanges to monitor
EXCHANGES = [
'ascendex',
'bequant',
'binance',
'binanceus',
'binanceusdm',
'bingx',
'bitfinex',
'bitfinex1',
'bitget', 
'bitmart', 
'bitmex', 
# 'bitopro', this one fucked up
'bitrue',
'bitstamp',
'blockchaincom',
# 'blofin', no markets
'bybit',
'cryptocom',
'deribit', 
'exmo',
'gate',
'gateio',
# 'gemini', something fucked up with market loading
'hashkey',
'hitbtc',
'kraken',
# 'krakenfutures', no markets
'kucoin',
'myokx',
# 'okx', same market error as gemini
'p2b',
'phemex',
'poloniex',
# 'poloniexfutures', no markets
'probit',
'upbit',
'whitebit',
'woo',
'xt',
]

class PriceTracker:
    def __init__(self):
        self.price_data = defaultdict(lambda: defaultdict(dict))  # {token: {exchange: {pair: price}}}
        self.exchanges = {}
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
        """Initialize MongoDB connection and exchange instances."""
        logger.info("Initializing Price Tracker...")
        
        # Initialize MongoDB
        try:
            self.client = MongoClient(MONGO_URL)


            # Test connection
            self.client.admin.command('ping')
            logger.info("✅ MongoDB connection established")
        except Exception as e:
            logger.error(f"❌ Failed to connect to MongoDB: {e}")
            raise
        
        # Initialize exchanges
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
                logger.error(f"❌ Failed to initialize {exchange_id}: {e}")
        
        logger.info(f"🏦 Successfully initialized {len(successful_exchanges)} exchanges")
        return len(successful_exchanges) > 0
    
    async def get_available_pairs(self, exchange, exchange_id: str) -> List[str]:
        """Get available trading pairs for an exchange."""
        try:
            await exchange.load_markets()
            available_pairs = []
            
            for token in TOKENS:
                for quote in QUOTE_CURRENCIES:
                    if exchange_id == "htx" and quote == "USDC":
                        pass

                    symbol = f"{token}/{quote}"
                    if symbol in exchange.markets:
                        available_pairs.append(({"base": token, "quote": quote, "market": symbol}))
            
            logger.info(f"📊 {exchange_id}: Found {len(available_pairs)} available pairs")
            return available_pairs
            
        except Exception as e:
            logger.error(f"❌ Error loading markets for {exchange_id}: {e}")
            return []
    
    async def watch_ticker(self, exchange, exchange_id: str, symbol: str):
        """Watch ticker for a specific symbol on an exchange - runs once to establish the watch."""
        tickerId = f"{exchange_id}_{symbol}_ticker_py"
        try:
            await exchange.watch_ticker(symbol)
            self.trackedStuff.append({"exchange": exchange, "method":"ticker", "symbol":symbol, "source": tickerId})
            # print(f"appending {tickerId} {symbol} ticker")

        except Exception as e:
            logger.error(f"❌ Error establishing ticker watch for {exchange_id} {symbol}: {e}")

    async def watch_orderbook(self, exchange, exchange_id: str, symbol: str):
        """Watch orderbook for a specific symbol on an exchange - runs once to establish the watch."""
        try:
            if not exchange.has.get('watchOrderBook'):
                return
            
            default_limit = 20
            limit_overrides = {
                "bitfinex": 25, "bitfinex1": 25, "bitmex": 25,
                "bybit": 1, "kraken": 10, "poloniexfutures": 5
            }
            limit = limit_overrides.get(exchange.id, default_limit) 

            orderbookId = f"{exchange_id}_{symbol}_orderbook_py"
            
            await exchange.watch_order_book(symbol, limit)
            self.trackedStuff.append({"exchange": exchange, "method": "orderbook", "symbol":symbol, "source": orderbookId})
            # print(f"appending {orderbookId} {symbol} orderbook")

        except Exception as e:
            logger.error(f"❌ Error establishing orderbook watch for {exchange_id} {symbol}: {e}")
    
    async def start_exchange_watchers(self, exchange, exchange_id: str):
        """Start all watchers for an exchange."""
        pairs = await self.get_available_pairs(exchange, exchange_id)
        if not pairs:
            logger.warning(f"⚠️ No pairs available for {exchange_id}")
            return
        
        exchange_tasks = []
        for pair in pairs:
            symbol = pair['market']
            # Create ticker watcher
            if exchange.has.get('watchTicker'):
                task = asyncio.create_task(self.watch_ticker(exchange, exchange_id, symbol))
                exchange_tasks.append(task)
            
            # Create orderbook watcher
            if exchange.has.get('watchOrderBook'):
                task = asyncio.create_task(self.watch_orderbook(exchange, exchange_id, symbol))
                exchange_tasks.append(task)
        
        logger.info(f"👀 Started {len(exchange_tasks)} watchers for {exchange_id}")
        self.tasks.extend(exchange_tasks)
        
        try:
            await asyncio.gather(*exchange_tasks, return_exceptions=True)
        except Exception as e:
            logger.error(f"❌ Error in exchange watchers for {exchange_id}: {e}")
    
    def get_price_collection(self, symbol):
        if not self.client:  # Initialize MongoDB client if not already connected
            self.client = MongoClient(MONGO_URL)
        
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
                
                # Take a snapshot of current data and clear it atomically

                priceDict = {}

                for item in self.trackedStuff:
                    # print(f"test item {item}")
                    exchange = item.get("exchange")
                    method = item.get("method")
                    symbol = item.get("symbol")
                    source = item.get("source")
                    token = symbol.split('/')[0]
                    # print(f"test {exchange} {method} {symbol}")
                    if method == "ticker":
                        ticker = exchange.tickers[symbol]
                        
                        if "last" in ticker and ticker["last"] is not None:
                            price = float(ticker["last"])
                            if token not in priceDict:
                                priceDict[token] = []

                            priceDict[token].append({"sourceId": f"{source}_last","price": price})

                        if "average" in ticker and ticker["average"] is not None:
                            price = float(ticker["average"])
                            if token not in priceDict:
                                priceDict[token] = []
                            priceDict[token].append({"sourceId": f"{source}_average","price": price})

                    elif method == "orderbook":
                        orderbook = exchange.orderbooks[symbol]
                        # print(f"testorderbook  {orderbook}")
                        if orderbook and orderbook.get('bids') and orderbook.get('asks'):
                            bid = orderbook['bids'][0][0] if orderbook['bids'] else None
                            ask = orderbook['asks'][0][0] if orderbook['asks'] else None
                            
                            # print(f"test {exchange.orderbooks}")

                            if bid and ask:
                                mid_price = (bid + ask) / 2
                                if token not in priceDict:
                                    priceDict[token] = []
                                priceDict[token].append({"sourceId": source,"price": mid_price})

                sum = 0
                for token, exchanges_data in priceDict.items():
                    # if token == "BTC":
                    collection = self.get_price_collection(token)

                    # collection.update_one(
                    #     {'timestamp': self.current_timestamp(), 'sourceId': self.general_source_id},
                    #     {'$set': {'prices': exchanges_data}},
                    #     upsert=True
                    # )
                    print(f"test {token} {exchanges_data.__len__()}  ")
                    sum += exchanges_data.__len__()

                save_time = time.time() - start_time
                print(f"{self.current_timestamp()} saved {sum} in {save_time:.3f}s")
                
                elapsed = time.time() - start_time
                sleep_time = max(0, 1.0 - elapsed)
                print(f"sleeping {sleep_time:.3f}s")
                if sleep_time > 0:
                    await asyncio.sleep(sleep_time)
                else:
                    logger.warning(f"⚠️ Save operation took {elapsed:.3f}s (longer than 1s interval)")
                
            except Exception as e:
                logger.error(f"❌ Error saving to MongoDB: {e}")
                await asyncio.sleep(1)
    
    async def run(self):
        """Main run method."""
        if not await self.initialize():
            logger.error("❌ Failed to initialize. Exiting.")
            return
        
        try:
            # Start exchange watchers
            exchange_tasks = []
            for exchange_id, exchange in self.exchanges.items():
                task = asyncio.create_task(self.start_exchange_watchers(exchange, exchange_id))
                exchange_tasks.append(task)
            
            # Start MongoDB save task
            save_task = asyncio.create_task(self.save_to_mongo())
            
            # Combine all tasks
            all_tasks = exchange_tasks + [save_task]
            self.tasks.extend(all_tasks)
            
            logger.info("🚀 All systems running! Press Ctrl+C to stop.")
            
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
        
        # Close exchanges
        if self.exchanges:
            logger.info("🏦 Closing exchanges...")
            close_tasks = []
            for exchange_id, exchange in self.exchanges.items():
                try:
                    close_tasks.append(exchange.close())
                except Exception as e:
                    logger.error(f"❌ Error closing {exchange_id}: {e}")
            
            if close_tasks:
                await asyncio.gather(*close_tasks, return_exceptions=True)
        
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
    if tracker:
        asyncio.create_task(tracker.cleanup())

async def main():
    """Main function."""
    global tracker
    
    # Set up signal handlers
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    # Create and run tracker
    tracker = PriceTracker()
    
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



    async def save_to_mongo(self):
        """Save current price data to MongoDB every second using bulk operations."""
        logger.info("💾 Started MongoDB save task")
        
        while not self.shutdown_event.is_set():
            try:
                start_time = time.time()
                
                # Take a snapshot of current data and clear it atomically

                priceDict = {}

                for item in self.trackedStuff:
                    # print(f"test item {item}")
                    exchange = item.get("exchange")
                    method = item.get("method")
                    symbol = item.get("symbol")
                    source = item.get("source")
                    token = symbol.split('/')[0]
                    # print(f"test {exchange} {method} {symbol}")
                    if method == "ticker":
                        ticker = exchange.tickers[symbol]
                        
                        if "last" in ticker and ticker["last"] is not None:
                            price = float(ticker["last"])
                            if token not in priceDict:
                                priceDict[token] = []

                            priceDict[token].append({"sourceId": f"{source}_last","price": price})

                        if "average" in ticker and ticker["average"] is not None:
                            price = float(ticker["average"])
                            if token not in priceDict:
                                priceDict[token] = []
                            priceDict[token].append({"sourceId": f"{source}_average","price": price})

                    elif method == "orderbook":
                        orderbook = exchange.orderbooks[symbol]
                        # print(f"testorderbook  {orderbook}")
                        if orderbook and orderbook.get('bids') and orderbook.get('asks'):
                            bid = orderbook['bids'][0][0] if orderbook['bids'] else None
                            ask = orderbook['asks'][0][0] if orderbook['asks'] else None
                            
                            # print(f"test {exchange.orderbooks}")

                            if bid and ask:
                                mid_price = (bid + ask) / 2
                                if token not in priceDict:
                                    priceDict[token] = []
                                priceDict[token].append({"sourceId": source,"price": mid_price})

                sum = 0
                for token, exchanges_data in priceDict.items():
                    # if token == "BTC":
                    collection = self.get_price_collection(token)

                    # collection.update_one(
                    #     {'timestamp': self.current_timestamp(), 'sourceId': self.general_source_id},
                    #     {'$set': {'prices': exchanges_data}},
                    #     upsert=True
                    # )
                    print(f"test {token} {exchanges_data.__len__()}  ")
                    sum += exchanges_data.__len__()

                save_time = time.time() - start_time
                print(f"{self.current_timestamp()} saved {sum} in {save_time:.3f}s")
                
                elapsed = time.time() - start_time
                sleep_time = max(0, 1.0 - elapsed)
                print(f"sleeping {sleep_time:.3f}s")
                if sleep_time > 0:
                    await asyncio.sleep(sleep_time)
                else:
                    logger.warning(f"⚠️ Save operation took {elapsed:.3f}s (longer than 1s interval)")
                
            except Exception as e:
                logger.error(f"❌ Error saving to MongoDB: {e}")
                await asyncio.sleep(1)