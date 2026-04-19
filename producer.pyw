from kafka import KafkaProducer
import json
import random
import time
from datetime import datetime

producer = KafkaProducer(
    bootstrap_servers='broker:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

stores = ["Warszawa", "Kraków", "Gdańsk", "Wrocław"]
categories = ["elektronika", "odzież", "żywność", "książki"]

tx_counter = 1

def generate_transaction():
    global tx_counter

    transaction = {
        "tx_id": f"TX{tx_counter:04d}",
        "user_id": f"u{random.randint(1, 20):02d}",
        "amount": round(random.uniform(5.0, 5000.0), 2),
        "store": random.choice(stores),
        "category": random.choice(categories),
        "timestamp": datetime.now().isoformat()
    }

    tx_counter += 1
    return transaction

while True:
    tx = generate_transaction()
    producer.send("transactions", value=tx)
    producer.flush()
    print(f"Wysłano: {tx}")
    time.sleep(1)
