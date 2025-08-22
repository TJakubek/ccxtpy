from pymongo import MongoClient
from collections import defaultdict
from threading import Timer
from datetime import datetime
import os

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
        self.save_timer = None  # Save timer reference

    def clear(self):
        self.store.clear()

    def is_blacklisted(self, token, source_id):
        return any(el.lower() in source_id.lower() for el in self.blacklist.get(token, []))

    def update_price(self, token, source_id, price):
        if self.is_blacklisted(token, source_id):
            return

        if not self.prices_initiated:
            self.save()  # Start the save loop if not already started
            self.prices_initiated = True

        if token not in self.store:
            self.store[token] = {}

        new_price = float(price)
        if source_id not in self.store[token] or new_price != float(self.store[token][source_id]):
            self.store[token][source_id] = new_price

    def save(self):
        def save_task():
            timestamp = self.current_timestamp()
            total_updates = 0  # Tracks total updates per tick

            # Create a copy to avoid modifying the dictionary while iterating
            store_copy = list(self.store.items())

            for token, source_prices in store_copy:
                num_updates = len(source_prices)  # Count updates for this token
                total_updates += num_updates  # Add to total count
                
                formatted_prices = [{'sourceId': id_, 'price': price} for id_, price in source_prices.items()]

                if formatted_prices:
                    collection = self.get_price_collection(token)
                    # collection.update_one(
                    #     {'timestamp': timestamp, 'sourceId': self.general_source_id},
                    #     {'$set': {'prices': formatted_prices}},
                    #     upsert=True
                    # )

                print(f"{token}: {num_updates} updates ")
                self.store[token] = {}  # Clear the stored prices

            print(f"Total updates this tick: {total_updates}")  # Print total updates

            # Re-schedule the task every 1 second
            self.save_timer = Timer(1.0, save_task)
            self.save_timer.start()

        # Start the save loop if not already running
        self.save_timer = Timer(1.0, save_task)
        self.save_timer.start()

    @staticmethod
    def current_timestamp():
        """Returns the current UTC timestamp in whole seconds."""
        return round(datetime.utcnow().timestamp())

    def get_price_collection(self, symbol):
        mongo_url = os.getenv("MONGO_SERVER")
        
        if not self.client:  # Initialize MongoDB client if not already connected
            self.client = MongoClient(mongo_url)
        
        db = self.client["tso"]

        if symbol not in self.price_collections:
            self.price_collections[symbol] = db[f"prices-{symbol}"]

        return self.price_collections[symbol]

    def close(self):
        """Close the MongoDB connection and stop the save timer."""
        if self.save_timer:
            print("Stopping save timer.")
            self.save_timer.cancel()
            self.save_timer = None

        if self.client:
            print("Closing MongoDB connection.")
            self.client.close()
            self.client = None
