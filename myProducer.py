from confluent_kafka import Producer
import json
import random
import time

# Configuration du producteur Kafka
producer_config = {
    'bootstrap.servers': 'pkc-e0zxq.eu-west-3.aws.confluent.cloud:9092',
    'security.protocol': 'SASL_SSL',
    'sasl.mechanisms': 'PLAIN',
    'sasl.username': 'JE36TZGOA2SWA532',
    'sasl.password': 'N/5VbEftA+mbgKQlBeXkXO9ZeaQOpAJuijIRUDcdseo1hfG4MxCRRG56L2NS4VTR'
}

producer = Producer(producer_config)

topic_users = 'TD5_users'
topic_transactions = 'TD5_transactions'

def generate_user_data():
    return {
        "user_id": str(random.randint(1000, 9999)),
        "name": f"user_{random.randint(1, 100)}",
        "country": random.choice(["France", "Maroc", "USA", "Canada", "Germany", "Japan", "Australia"])
    }

def generate_transaction_data():
    return {
        "user_id": str(random.randint(1000, 9999)),
        "item_name": f"item_{random.randint(1, 50)}",
        "item_type": random.choice(["electronics", "clothing", "food", "books", "furniture"]),
        "unit_price": round(random.uniform(10.0, 500.0), 2),
        "amount": random.randint(1, 10),
        "timestamp": time.time()
    }

def delivery_report(err, msg):
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}]")

def publish_messages(rate):
    while True:
        try:
            user_data = generate_user_data()
            transaction_data = generate_transaction_data()

            producer.produce(topic_users, json.dumps(user_data), callback=delivery_report)
            producer.produce(topic_transactions, json.dumps(transaction_data), callback=delivery_report)

            producer.flush()
            time.sleep(1 / rate)
        except Exception as e:
            print(f"Error: {e}")

#rate = random.randint(1, 10)
rate=50
publish_messages(rate)
