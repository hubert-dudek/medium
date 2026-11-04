import sys

marker = "NEWS_SERVERLESS_LOGS_20261104_A7F3"
orders = [
    {"order_id": 1, "delivery_address": "Demo address 1"},
    {"order_id": 2, "delivery_address": "Demo address 2"},
    {"order_id": 3, "delivery_address": None},
]

print(marker, flush=True)
print(f"Order import completed: {len(orders)} rows", flush=True)
missing_addresses = sum(order["delivery_address"] is None for order in orders)
print(f"WARNING: {missing_addresses} order has no delivery address", file=sys.stderr, flush=True)
