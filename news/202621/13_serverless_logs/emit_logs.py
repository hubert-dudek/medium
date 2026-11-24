import sys

orders = [
    {"order_id": 1, "delivery_address": "Demo address 1"},
    {"order_id": 2, "delivery_address": "Demo address 2"},
    {"order_id": 3, "delivery_address": None},
]

print(f"NEWS_202621 Order import completed: {len(orders)} rows", flush=True)
missing_addresses = sum(order["delivery_address"] is None for order in orders)
print(f"NEWS_202621 Missing delivery addresses: {missing_addresses}", file=sys.stderr, flush=True)
