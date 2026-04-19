from kafka import KafkaConsumer
from collections import Counter
import json

consumer = KafkaConsumer(
    'transactions',
    bootstrap_servers='broker:9092',
    value_deserializer=lambda x: json.loads(x.decode('utf-8')),
    group_id='transaction-count-group',
    auto_offset_reset='earliest'
)

store_counts = Counter()
total_amount = {}
msg_count = 0

print("Zliczam transakcje per sklep...")

for message in consumer:
    event = message.value
    store = event["store"]
    amount = event["amount"]

    store_counts[store] += 1
    total_amount[store] = total_amount.get(store, 0) + amount
    msg_count += 1

    print(f"Odebrano #{msg_count}: {event['tx_id']} | {store} | {amount:.2f}")

    if msg_count % 1 == 0:
        print("\n--- PODSUMOWANIE ---")
        print(f"{'Sklep':<12} {'Liczba':<8} {'Suma':<12} {'Średnia':<12}")
        print("-" * 48)

        for store_name in store_counts:
            count = store_counts[store_name]
            total = total_amount[store_name]
            avg = total / count if count > 0 else 0
            print(f"{store_name:<12} {count:<8} {total:<12.2f} {avg:<12.2f}")
