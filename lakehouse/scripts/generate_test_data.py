import argparse
import random
import string
from datetime import datetime

from pymongo import MongoClient


def random_id(prefix: str) -> str:
    return f"{prefix}-{''.join(random.choices(string.ascii_lowercase + string.digits, k=6))}"


def seed(uri: str, database: str):
    client = MongoClient(uri)
    db = client[database]
    orders = db.orders
    for _ in range(10):
        doc = {
            "order_id": random_id("ord"),
            "merchant_id": random.choice(["m-1", "m-2", "m-3"]),
            "status": random.choice(["created", "approved", "refunded"]),
            "amount": round(random.uniform(10, 500), 2),
            "currency": "USD",
            "event_ts": datetime.utcnow().isoformat(),
            "attributes": {
                "channel": random.choice(["web", "mobile"]),
                "promo": random.choice(["SUMMER", "WINTER", None]),
            },
        }
        orders.update_one({"order_id": doc["order_id"]}, {"$set": doc}, upsert=True)

    client.close()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--uri", default="mongodb://mongo:mongo@mongo:27017/")
    parser.add_argument("--database", default="payments")
    args = parser.parse_args()
    seed(args.uri, args.database)


if __name__ == "__main__":
    main()
