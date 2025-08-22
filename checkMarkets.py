#!/usr/bin/env python3
"""
CCXT Websocket Price Tracker
Tracks cryptocurrency prices across multiple exchanges using websockets
and saves data to MongoDB every second.
"""

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
DB_NAME = "crypto_prices"
COLLECTION_NAME = "prices"

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
    # "binance",
    # "coinbase", 
    # "kraken",
    # "okx",
    # "bybit",
    # "kucoin",
    # "gate",
    # "bitget",
    # "mexc",
    "htx"
]

class PriceTracker:
    def __init__(self):
        self.price_data = defaultdict(lambda: defaultdict(dict))  # {token: {exchange: {pair: price}}}
        self.exchanges = {}
        self.mongo_client = None
        self.db = None
        self.collection = None
        self.shutdown_event = asyncio.Event()
        self.tasks = []
        self.lock = asyncio.Lock()
        
    def current_timestamp(self):
        """Returns the current UTC timestamp in whole seconds."""
        return round(datetime.utcnow().timestamp())

    async def initialize(self):
        """Initialize MongoDB connection and exchange instances."""
        logger.info("Initializing Price Tracker...")
        
        # Initialize MongoDB
        try:
            self.mongo_client = MongoClient(MONGO_URL)
            self.db = self.mongo_client[DB_NAME]
            self.collection = self.db[COLLECTION_NAME]
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
            
            # print(f"exchange markets: {exchange.markets}")

            for token in TOKENS:
                for quote in QUOTE_CURRENCIES:
                    symbol = f"{token}/{quote}"
                    if symbol in exchange.markets:
                        print(f" symbol: {symbol} {exchange.markets[symbol]}")
                        available_pairs.append(({"base": token, "quote": quote, "market": symbol}))
                    else:
                        print(f" symbol: {symbol} not in markets")
            
            logger.info(f"📊 {exchange_id}: Found {len(available_pairs)} available pairs")
            return available_pairs
            
        except Exception as e:
            logger.error(f"❌ Error loading markets for {exchange_id}: {e}")
            return []
    
    async def watch_ticker(self, exchange, exchange_id: str, symbol: str):
        """Watch ticker for a specific symbol on an exchange."""
        tickerId = f"{exchange_id}_{symbol}_ticker"
        try:
            while not self.shutdown_event.is_set():
                try:
                    ticker = await exchange.watch_ticker(symbol)
                    if ticker and ticker.get('last'):
                        token = symbol.split('/')[0]
                        price = float(ticker['last'])
                        timestamp = datetime.utcnow()
                        
                        if token not in self.price_data:
                            self.price_data[token] = defaultdict(dict)

                        async with self.lock:
                            self.price_data[token][tickerId] = price
                        
                        logger.debug(f"📈 {exchange_id} {symbol}: ${price:.6f}")
                        

                except Exception as e:
                    if not self.shutdown_event.is_set():
                        logger.error(f"❌ Error watching {exchange_id} ticker {symbol}: {e}")
                        await asyncio.sleep(10)
                        
        # except asyncio.CancelledError:
            # logger.info(f"🛑 Stopped watching {exchange_id} {symbol}")
        except Exception as e:
            logger.error(f"❌ Fatal error watching {exchange_id} ticker {symbol}: {e}")
    
    async def watch_orderbook(self, exchange, exchange_id: str, symbol: str):
        """Watch orderbook for a specific symbol on an exchange."""
        try:
            if not exchange.has.get('watchOrderBook'):
                return
            
            default_limit = 20
            limit_overrides = {
                "bitfinex": 25, "bitfinex1": 25, "bitmex": 25,
                "bybit": 1, "kraken": 10, "poloniexfutures": 5
            }
            limit = limit_overrides.get(exchange.id, default_limit) 

            orderbookId = f"{exchange_id}_{symbol}_orderbook"

            while not self.shutdown_event.is_set():
                try:
                    orderbook = await exchange.watch_order_book(symbol, limit)
                    if orderbook and orderbook.get('bids') and orderbook.get('asks'):
                        token = symbol.split('/')[0]
                        bid = orderbook['bids'][0][0] if orderbook['bids'] else None
                        ask = orderbook['asks'][0][0] if orderbook['asks'] else None
                        
                        if bid and ask:
                            mid_price = (bid + ask) / 2
                            timestamp = datetime.utcnow()
                            
                            async with self.lock:
                                if token not in self.price_data:
                                    self.price_data[token] = defaultdict(dict)
                                    
                                # Update existing data or create new entry
                                self.price_data[token][orderbookId] = mid_price
                            
                            logger.debug(f"📊 {exchange_id} {symbol} orderbook: ${mid_price:.6f}")
                        

                except Exception as e:
                    if not self.shutdown_event.is_set():
                        logger.error(f"❌ Error watching {exchange_id} {symbol} orderbook: {e}")
                        await asyncio.sleep(10)
                        
        except asyncio.CancelledError:
            # logger.info(f"🛑 Stopped watching {exchange_id} {symbol} orderbook")
            pass
        except Exception as e:
            logger.error(f"❌ Fatal error watching {exchange_id} {symbol} orderbook: {e}")
    
    async def start_exchange_watchers(self, exchange, exchange_id: str):
        """Start all watchers for an exchange."""
        pairs = await self.get_available_pairs(exchange, exchange_id)
        if not pairs:
            logger.warning(f"⚠️ No pairs available for {exchange_id}")
            return
        
        # exchange_tasks = []
        # # pairs is a list of dictionaries with keys 'base', 'quote', and 'market'
        # # loop over markets
        # for pair in pairs:
        #     symbol = pair['market']
        #     # Create ticker watcher
        #     if exchange.has.get('watchTicker'):
        #         task = asyncio.create_task(self.watch_ticker(exchange, exchange_id, symbol))
        #         exchange_tasks.append(task)
            
        #     # Create orderbook watcher
        #     if exchange.has.get('watchOrderBook'):
        #         task = asyncio.create_task(self.watch_orderbook(exchange, exchange_id, symbol))
        #         exchange_tasks.append(task)
        
        # logger.info(f"👀 Started {len(exchange_tasks)} watchers for {exchange_id}")
        # self.tasks.extend(exchange_tasks)
        
        # try:
        #     await asyncio.gather(*exchange_tasks, return_exceptions=True)
        # except Exception as e:
        #     logger.error(f"❌ Error in exchange watchers for {exchange_id}: {e}")
    
    async def save_to_mongo(self):
        """Save current price data to MongoDB every second."""
        logger.info("💾 Started MongoDB save task")
        
        while not self.shutdown_event.is_set():
            try:
                start_time = time.time()
                
                # async with self.lock:
                #     # Create a snapshot of current data
                #     data_snapshot = {}
                #     total_pairs = 0
                    
                #     for token, exchanges_data in self.price_data.items():
                #         data_snapshot[token] = {}
                #         for exchange_id in exchanges_data.items():
                #             if pairs_data:  # Only include exchanges with data
                #                 data_snapshot[token][exchange_id] = dict(pairs_data)
                #                 total_pairs += len(pairs_data)
                
                # if data_snapshot:
                #     # Prepare document for MongoDB
                #     document = {
                #         'timestamp': datetime.utcnow(),
                #         'data': data_snapshot,
                #         'summary': {
                #             'total_tokens': len(data_snapshot),
                #             'total_pairs': total_pairs,
                #             'exchanges': list(self.exchanges.keys())
                #         }
                #     }
                    
                #     # Insert into MongoDB
                #     # self.collection.insert_one(document)
                #     print(f"data snapshot: {data_snapshot}")

                #     # clear price_data after saving
                #     self.price_data.clear()
                    
                #     save_time = time.time() - start_time
                #     logger.info(f"💾 Saved {len(data_snapshot)} tokens, {total_pairs} pairs to MongoDB ({save_time:.3f}s)")
                # else:
                #     logger.warning("⚠️ No data to save to MongoDB")
                    # Prepare document for MongoDB
                # if data_snapshot:
                sumOfDocuments = 0
                for token, exchanges_data in self.price_data.items():
                    # print(f"data snapshot: {token} {exchanges_data}")
                    document = {
                        'timestamp': self.current_timestamp(),
                        'token': token,
                        'exchanges_data': []
                    }
                    

                    for exchange_id, price in exchanges_data.items():
                        exchange_data = {
                            'exchange_id': exchange_id,
                            'price': price,
                        }
                        document['exchanges_data'].append(exchange_data)
                        sumOfDocuments += 1

                    # print(f"data snapshot: {token} {document}")
                    # Insert into MongoDB
                    # self.collection.insert_one(document)
                    if token == 'BTC':
                        print(f"data snapshot:   {document}")

                # clear price_data after saving
                self.price_data.clear()
                
                save_time = time.time() - start_time
                logger.info(f"💾 Saved {sumOfDocuments} tokens")
                # else:
                #     logger.warning("⚠️ No data to save to MongoDB")
                
                # Wait for next second
                elapsed = time.time() - start_time
                sleep_time = max(0, 1.0 - elapsed)
                await asyncio.sleep(sleep_time)
                
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
        if self.mongo_client:
            logger.info("🔌 Closing MongoDB connection...")
            self.mongo_client.close()
        
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

