from kafka import KafkaConsumer
from collections import defaultdict, deque
from datetime import datetime, timedelta
import json

consumer = KafkaConsumer(
    'transactions',
    bootstrap_servers='broker:9092',
    value_deserializer=lambda x: json.loads(x.decode('utf-8')),
    group_id='transaction-anomaly-group',
    auto_offset_reset='earliest'
)

user_transactions = defaultdict(deque)

print("Wykrywam anomalie: > 3 transakcje w ciągu 60 sekund...")

for message in consumer:
    event = message.value
    user_id = event["user_id"]
    tx_time = datetime.fromisoformat(event["timestamp"])

    history = user_transactions[user_id]
    history.append(tx_time)

    limit = tx_time - timedelta(seconds=60)
    while history and history[0] < limit:
        history.popleft()

    if len(history) > 3:
        print(
            f"ALERT ANOMALY: user {user_id} wykonał "
            f"{len(history)} transakcje w ciągu 60 sekund "
            f"(ostatnia: {event['tx_id']}, kwota: {event['amount']:.2f} PLN)"
        )
