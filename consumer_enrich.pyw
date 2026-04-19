from kafka import KafkaConsumer
import json

consumer = KafkaConsumer(
    'transactions',
    bootstrap_servers='broker:9092',
    value_deserializer=lambda x: json.loads(x.decode('utf-8')),
    group_id='transaction-enrich-group',
    auto_offset_reset='earliest'
)

print("Nasłuchuję i wzbogacam transakcje o risk_level...")

for message in consumer:
    event = message.value
    amount = event["amount"]

    if amount > 3000:
        event["risk_level"] = "HIGH"
    elif amount > 1000:
        event["risk_level"] = "MEDIUM"
    else:
        event["risk_level"] = "LOW"

    print(event)
