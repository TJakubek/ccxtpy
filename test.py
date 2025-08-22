import os
import asyncio
import ccxt.pro as ccxt
from pymongo import MongoClient
from dotenv import load_dotenv
from priceStore import PriceStore
import signal

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
# EXCHANGES = ["xt"]  # Add more exchanges as needed
# EXCHANGES = ["kucoin", "binance", "mexc"]  # Add more exchanges as needed

# 'coinbase', rate limited
# 'coinex', doesnt connect to more symbol than 1
# 'defx', only usdc
# 'hyperliquid', only usdc
# 'hollaex',  kinda shitty, loses trades
# 'idex', error fetching markets
# 'ndax', some iterable bool error
# 'okcoin', some fetch error
# 'oxfun', no markets
# 'paradex', usdc markets
# 'vertex', usdc markets
# 'woofipro', usdc markets

price_store = PriceStore()
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
        quote_list = ["USD", "USDT", "USD:USD", "USDT:USDT"]

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
                tasks.append(watch_order_books(exchange, market_symbol, base))
                tasks.append(watch_trades(exchange, market_symbol, base))
                tasks.append(watch_ticker(exchange, market_symbol, base))

    if tasks:
        if hasattr(asyncio, "TaskGroup"):  # Python 3.11+ structured concurrency
            async with asyncio.TaskGroup() as tg:
                for task in tasks:
                    tg.create_task(task)
        else:
            await asyncio.gather(*tasks)


async def main():
    """Main function to initialize exchanges and start watchers."""
    symbols = await fetch_symbols_from_mongo()
    exchange_instances = {eid: getattr(ccxt, eid)() for eid in EXCHANGES}

    try:
        results = await asyncio.gather(
            *[fetch_markets_with_usd(exchange_instances[eid], symbols) for eid in EXCHANGES]
        )

        exchange_tasks = [
            watch_for_exchange(exchange_instances[eid], markets, symbols)
            for eid, markets in zip(EXCHANGES, results)
        ]

        await asyncio.gather(*exchange_tasks)

    except asyncio.CancelledError:
        print("\n⚠️ Script interrupted. Cleaning up...")

    finally:
        print("🔄 Closing exchanges...")
        price_store.close()
        for exchange in exchange_instances.values():
            try:
                await exchange.close()
                if exchange.session:
                    await exchange.session.close()
            except Exception as e:
                print(f"⚠️ Error closing exchange {exchange.id}: {e}")

        print("✅ All exchanges closed successfully.")


def signal_handler(sig, frame):
    print("\n⚠️ Interrupt signal received! Shutting down...")
    for task in asyncio.all_tasks():
        task.cancel()


signal.signal(signal.SIGINT, signal_handler)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n❌ Script interrupted by user.")
    except Exception as e:
        print(f"🚨 Unexpected error: {e}")
    finally:
        print("🛑 Cleaning up resources...")
        price_store.close()