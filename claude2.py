import os
import asyncio
import ccxt.pro as ccxt
from pymongo import MongoClient
from dotenv import load_dotenv
import signal
from collections import defaultdict
from datetime import datetime
import time


# Load environment variables from .env file
load_dotenv()

# MongoDB Configuration
MONGO_URL = os.getenv("MONGO_SERVER")
DB_NAME = "tso"
COLLECTION_NAME = "symbols"
general_source_id = 'ccxtPy'

# List of Exchanges
EXCHANGES = [ 'ascendex',
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
 'bitopro',
 'bitrue',
 'bitstamp',
 'blockchaincom',
 'blofin',
 'bybit',
 'cryptocom',
 'deribit', 
 'exmo',
 'gate',
 'gateio',
 'gemini',
 'hashkey',
 'hitbtc',
 'htx',
 'huobi',
 'kraken',
 'krakenfutures',
 'kucoin',
 'myokx',
 'okx',
 'p2b',
 'phemex',
 'poloniex',
 'poloniexfutures',
 'probit',
 'upbit',
 'whitebit',
 'woo',
 'xt',
 ]  # Add more exchanges as needed

quote_list = ["USD", "USDT", "USD:USD", "USDT:USDT"]

class PriceStore:
    def __init__(self):
        self.store = defaultdict(dict)  # Stores prices per token
        self.prices_initiated = False
        self.general_source_id = 'ccxtPy'
        self.blacklist = {
            'LEO': ['mexc'],
            'TRUMP': ['poloniex', 'phemex', 'bitmex']
        }
        self.price_collections = {}
        self.client = None  # MongoDB client, initialize as None
        self.save_task = None  # Save task reference
        self.is_running = True  # Flag to control saving loop
        self.next_tick_time = 0  # Next scheduled tick time
        self.pending_saves = []  # Store pending save tasks
        self.save_queue = asyncio.Queue()  # Queue for asynchronous saving

    def clear(self):
        self.store.clear()

    def is_blacklisted(self, token, source_id):
        return any(el.lower() in source_id.lower() for el in self.blacklist.get(token, []))

    def update_price(self, token, source_id, price):
        if self.is_blacklisted(token, source_id):
            return

        if not self.prices_initiated:
            self.start_save_loop()  # Start the save loop if not already started
            self.prices_initiated = True

        if token not in self.store:
            self.store[token] = {}

        new_price = float(price)
        if source_id not in self.store[token] or new_price != float(self.store[token][source_id]):
            self.store[token][source_id] = new_price

    async def save_to_db(self):
        """Worker function that processes save tasks from the queue"""
        while self.is_running:
            try:
                # Get the next save task from the queue
                save_item = await self.save_queue.get()
                token, formatted_prices = save_item
                
                try:
                    collection = self.get_price_collection(token)
                    # Uncomment this to enable actual MongoDB storage
                    # collection.update_one(
                    #     {'timestamp': self.current_timestamp(), 'sourceId': self.general_source_id},
                    #     {'$set': {'prices': formatted_prices}},
                    #     upsert=True
                    # )
                    
                    # Simulate DB operation for testing
                    await asyncio.sleep(0.01)  # Simulate IO operation
                    
                except Exception as e:
                    print(f"Error saving {token} prices to DB: {e}")
                
                # Mark the task as done
                self.save_queue.task_done()
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                print(f"Error in DB save worker: {e}")
                await asyncio.sleep(0.1)  # Prevent busy loop on error
    
    async def save_loop(self):
        """Async loop that saves data to MongoDB precisely every second"""
        # Start DB worker tasks
        worker_count = 4  # Adjust number of workers based on your needs
        workers = [asyncio.create_task(self.save_to_db()) for _ in range(worker_count)]
        
        # Initialize the first tick time to the next whole second
        self.next_tick_time = time.time()
        self.next_tick_time = self.next_tick_time - (self.next_tick_time % 1) + 1  # Round to next second
        
        while self.is_running:
            try:
                # Calculate time until next tick
                current_time = time.time()
                time_to_next_tick = self.next_tick_time - current_time
                
                if time_to_next_tick > 0:
                    # Wait precisely until the next tick time
                    await asyncio.sleep(time_to_next_tick)
                
                # Start of tick processing
                tick_start_time = time.time()
                
                # Take a snapshot of the store to avoid modifying while iterating
                store_snapshot = {}
                for token, source_prices in self.store.items():
                    if source_prices:  # Skip empty tokens
                        store_snapshot[token] = source_prices.copy()
                        self.store[token].clear()  # Clear immediately after copying
                
                # Process the data and queue for saving
                total_updates = sum(len(prices) for prices in store_snapshot.values())
                active_saves = 0
                
                for token, source_prices in store_snapshot.items():
                    num_updates = len(source_prices)
                    
                    if num_updates > 0:
                        formatted_prices = [{'sourceId': id_, 'price': price} for id_, price in source_prices.items()]
                        
                        # Queue this save operation
                        await self.save_queue.put((token, formatted_prices))
                        active_saves += 1
                        
                        print(f"{token}: {num_updates} updates queued")
                
                print(f"Total updates this tick: {total_updates} | Queue size: {self.save_queue.qsize()}")
                
                # Increment to the next scheduled tick time (exactly 1 second later)
                self.next_tick_time += 1.0
                
                # If we've fallen behind by more than a second, reset to the next second
                if time.time() > self.next_tick_time:
                    print("⚠️ Warning: Processing took too long, resetting tick schedule")
                    self.next_tick_time = time.time() - (time.time() % 1) + 1  # Round to next second
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                print(f"Error in save loop: {e}")
                # Still maintain the timing schedule even after errors
                self.next_tick_time += 1.0
        
        # Cancel all worker tasks when exiting
        for worker in workers:
            worker.cancel()

    def start_save_loop(self):
        """Start the async save loop task"""
        if not self.save_task or self.save_task.done():
            self.save_task = asyncio.create_task(self.save_loop())
            self.prices_initiated = True

    @staticmethod
    def current_timestamp():
        """Returns the current UTC timestamp in whole seconds."""
        return round(datetime.utcnow().timestamp())

    def get_price_collection(self, symbol):
        try:
            if not self.client:  # Initialize MongoDB client if not already connected
                self.client = MongoClient(MONGO_URL)
            
            db = self.client[DB_NAME]

            if symbol not in self.price_collections:
                self.price_collections[symbol] = db[f"prices-{symbol}"]

            return self.price_collections[symbol]
        except Exception as e:
            print(f"Error getting price collection for {symbol}: {e}")
            # Re-raise to be handled by caller
            raise

    async def close(self):
        """Close the MongoDB connection and stop the save task."""
        print("Closing price store...")
        
        self.is_running = False
        
        if self.save_task:
            print("Stopping save task.")
            self.save_task.cancel()
            try:
                await self.save_task
            except asyncio.CancelledError:
                pass
            self.save_task = None
        
        # Wait for the save queue to be empty (optional)
        try:
            if self.save_queue.qsize() > 0:
                print(f"Waiting for {self.save_queue.qsize()} pending DB operations to complete...")
                await asyncio.wait_for(self.save_queue.join(), timeout=5.0)
        except asyncio.TimeoutError:
            print("Timeout waiting for DB operations to complete")

        if self.client:
            print("Closing MongoDB connection.")
            self.client.close()
            self.client = None


async def fetch_symbols_from_mongo():
    """Fetch unique symbols from MongoDB."""
    client = MongoClient(MONGO_URL)
    db = client[DB_NAME]
    collection = db[COLLECTION_NAME]
    symbols = list(set(collection.distinct("symbol")))
    client.close()
    return symbols


async def watch_order_books(exchange, symbol, base_token, limit=20):
    """Watches order books and updates mid-price if valid."""
    if not exchange.has.get("watchOrderBook"):
        return

    limit_overrides = {
        "bitfinex": 25, "bitfinex1": 25, "bitmex": 25,
        "bybit": 1, "kraken": 10, "poloniexfutures": 5
    }
    limit = limit_overrides.get(exchange.id, limit)

    while True:
        try:
            orderbook = await exchange.watch_order_book(symbol, limit)
            if orderbook and orderbook.get("asks") and orderbook.get("bids"):
                ask_price = orderbook["asks"][0][0]
                bid_price = orderbook["bids"][0][0]

                if isinstance(ask_price, (int, float)) and isinstance(bid_price, (int, float)):
                    mid_price = (ask_price + bid_price) / 2
                    price_store.update_price(base_token, f"{general_source_id}_{exchange.id}_{symbol}_ob", mid_price)
        except Exception as e:
            print(f"⚠️ Order book error on {exchange.id} {symbol}: {e}")
            await asyncio.sleep(5)


async def watch_trades(exchange, symbol, base_token):
    """Watches trades and updates the latest trade price."""
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
    """Watches ticker data for a given market on the exchange."""
    if not exchange.has.get("watchTicker"):
        return

    while True:
        try:
            ticker = await exchange.watch_ticker(symbol)
            if ticker:
                if "last" in ticker and ticker["last"] is not None:
                    price_store.update_price(base_token, f"{general_source_id}_{exchange.id}_{symbol}_last", float(ticker["last"]))
                if "average" in ticker and ticker["average"] is not None:
                    price_store.update_price(base_token, f"{general_source_id}_{exchange.id}_{symbol}_avg", float(ticker["average"]))
        except Exception as e:
            print(f"⚠️ Ticker error on {exchange.id} {symbol}: {e}")
            await asyncio.sleep(5)


async def fetch_markets_with_usd(exchange, symbols):
    """Fetches markets with USD/USDT pairs."""
    await exchange.load_markets()
    matching_markets = {}

    for symbol in symbols:
        matching_markets.setdefault(symbol, [])
        
        # Skip specific cases
        if exchange.id in ["gate", "gateio"] and symbol == "USDT":
            continue

        for quote in quote_list:
            market_symbol = f"{symbol}/{quote}"
            if market_symbol in exchange.markets:
                base_token, quote_token = market_symbol.split("/")
                if base_token in symbols and quote_token in quote_list:
                    matching_markets[symbol].append({"base": base_token, "quote": quote_token, "market": exchange.markets[market_symbol]})

    return matching_markets


async def watch_for_exchange(exchange, markets, symbols):
    """Runs watchers for order book, trades, and tickers concurrently."""
    tasks = []

    for symbol, market_list in markets.items():
        for market_data in market_list:
            base = market_data["base"]
            market_symbol = market_data["market"]["symbol"]

            if base in symbols:
                # Create concurrent tasks for all three watch functions
                tasks.append(asyncio.create_task(watch_order_books(exchange, market_symbol, base)))
                tasks.append(asyncio.create_task(watch_trades(exchange, market_symbol, base)))
                tasks.append(asyncio.create_task(watch_ticker(exchange, market_symbol, base)))

    if tasks:
        # Wait for all tasks to complete (which they won't unless cancelled)
        await asyncio.gather(*tasks, return_exceptions=True)


# Initialize exchange instances and store them
price_store = PriceStore()
exchange_instances = {}

async def initialize_exchanges():
    """Initialize all exchange instances"""
    global exchange_instances
    exchange_instances = {eid: getattr(ccxt, eid)() for eid in EXCHANGES}
    return exchange_instances


async def cleanup_resources():
    """Cleanup all resources properly"""
    print("\n🔄 Cleaning up resources...")
    
    # Cancel all running tasks
    for task in asyncio.all_tasks():
        if task is not asyncio.current_task():
            task.cancel()
    
    # Close price store properly
    await price_store.close()
    
    # Close all exchange connections
    for exchange_id, exchange in exchange_instances.items():
        try:
            print(f"Closing exchange {exchange_id}...")
            await exchange.close()
            if exchange.session:
                await exchange.session.close()
        except Exception as e:
            print(f"⚠️ Error closing exchange {exchange_id}: {e}")
    
    print("✅ All resources closed successfully.")


async def main():
    """Main function to initialize exchanges and start watchers."""
    try:
        # Initialize exchanges
        await initialize_exchanges()
        
        # Fetch symbols and market data
        symbols = await fetch_symbols_from_mongo()
        
        # Get markets for each exchange
        results = await asyncio.gather(
            *[fetch_markets_with_usd(exchange_instances[eid], symbols) for eid in EXCHANGES]
        )
        
        # Start the price store save loop
        price_store.start_save_loop()
        
        # Create tasks for each exchange
        exchange_tasks = [
            watch_for_exchange(exchange_instances[eid], markets, symbols)
            for eid, markets in zip(EXCHANGES, results)
        ]
        
        # Run all exchange tasks concurrently
        await asyncio.gather(*exchange_tasks)
        
    except asyncio.CancelledError:
        print("\n⚠️ Script interrupted. Cleaning up...")
    except Exception as e:
        print(f"🚨 Unexpected error: {e}")
    finally:
        await cleanup_resources()


def signal_handler(sig, frame):
    print("\n⚠️ Interrupt signal received! Shutting down...")
    # Set an event to signal task cancellation
    for task in asyncio.all_tasks():
        task.cancel()


# Register signal handlers
signal.signal(signal.SIGINT, signal_handler)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n❌ Script interrupted by user.")
    except Exception as e:
        print(f"🚨 Unexpected error: {e}")