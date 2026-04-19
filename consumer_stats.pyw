from kafka import KafkaConsumer
from collections import defaultdict
import json

consumer = KafkaConsumer(
    'transactions',
    bootstrap_servers='broker:9092',
    value_deserializer=lambda x: json.loads(x.decode('utf-8')),
    group_id='transaction-stats-group',
    auto_offset_reset='earliest'
)

stats = defaultdict(lambda: {
    "count": 0,
    "sum": 0.0,
    "min": float("inf"),
    "max": float("-inf")
})

msg_count = 0

print("Zbieram statystyki per kategoria...")

for message in consumer:
    event = message.value
    category = event["category"]
    amount = event["amount"]

    stats[category]["count"] += 1
    stats[category]["sum"] += amount
    stats[category]["min"] = min(stats[category]["min"], amount)
    stats[category]["max"] = max(stats[category]["max"], amount)

    msg_count += 1

    if msg_count % 10 == 0:
        print("\n--- STATYSTYKI KATEGORII ---")
        print(f"{'Kategoria':<15} {'Liczba':<8} {'Przychód':<12} {'Min':<10} {'Max':<10}")
        print("-" * 60)

        for cat, values in stats.items():
            print(
                f"{cat:<15} "
                f"{values['count']:<8} "
                f"{values['sum']:<12.2f} "
                f"{values['min']:<10.2f} "
                f"{values['max']:<10.2f}"
            )
