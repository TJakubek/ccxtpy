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

    async def save_loop(self):
        """Async loop that saves data to MongoDB every second"""
        while self.is_running:
            start_time = time.time()
            
            # Create a copy to avoid modifying the dictionary while iterating
            store_copy = list(self.store.items())
            total_updates = 0

            for token, source_prices in store_copy:
                num_updates = len(source_prices)
                total_updates += num_updates
                
                formatted_prices = [{'sourceId': id_, 'price': price} for id_, price in source_prices.items()]

                if formatted_prices:
                    collection = self.get_price_collection(token)
                    # collection.update_one(
                    #     {'timestamp': self.current_timestamp(), 'sourceId': self.general_source_id},
                    #     {'$set': {'prices': formatted_prices}},
                    #     upsert=True
                    # )

                print(f"{token}: {num_updates} updates ")
                self.store[token] = {}  # Clear the stored prices

            print(f"Total updates this tick: {total_updates}")
            
            # Calculate how long to wait for next save (target 1 second intervals)
            elapsed = time.time() - start_time
            wait_time = max(0, 1.0 - elapsed)  # Ensure we wait at least a bit
            
            # Wait until next save interval
            await asyncio.sleep(wait_time)

    def start_save_loop(self):
        """Start the async save loop task"""
        self.save_task = asyncio.create_task(self.save_loop())
        self.prices_initiated = True

    @staticmethod
    def current_timestamp():
        """Returns the current UTC timestamp in whole seconds."""
        return round(datetime.utcnow().timestamp())

    def get_price_collection(self, symbol):
        if not self.client:  # Initialize MongoDB client if not already connected
            self.client = MongoClient(MONGO_URL)
        
        db = self.client[DB_NAME]

        if symbol not in self.price_collections:
            self.price_collections[symbol] = db[f"prices-{symbol}"]

        return self.price_collections[symbol]

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