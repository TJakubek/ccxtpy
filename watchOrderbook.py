async def main():
    """Main function to fetch symbols and start order book/ticker watchers."""
    exchange_objects = {exchange: getattr(ccxt, exchange)() for exchange in EXCHANGES}

    try:
        tasks = []

        for exchange_id, markets in zip(EXCHANGES, results):
        print(f"Exchange: {exchange_id}")
        # Launch a task for each exchange to process its markets concurrently
        tasks.append(watch_order_books_for_exchange(exchange_id, markets, symbols))

        await asyncio.gather(*tasks)  # Run all tasks concurrently

    except Exception as e:
        print(f"Unexpected error in main(): {e}")

    finally:
        # Ensure all exchanges are properly closed
        for exchange in exchange_objects.values():
            await exchange.close()

        # Ensure the price store is also closed if necessary
        price_store.close()