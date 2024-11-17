# bundling.py
from kafka import KafkaProducer, KafkaConsumer
import json
import logging

def bundle_debts():
    consumer = KafkaConsumer('processed_debts', bootstrap_servers='127.0.0.1:9092', consumer_timeout_ms=10000)
    producer = KafkaProducer(bootstrap_servers='127.0.0.1:9092',
                         request_timeout_ms=20000,
                         value_serializer=lambda v: json.dumps(v).encode('utf-8'))


    for msg in consumer:
        logging.info(f"Received debt message: {msg.value}")
        debt = json.loads(msg.value)
        # Example bundling logic
        bundle = {"total_debt": debt['amount'] * 1.1}  # Simple multiplier for illustration
        producer.send('bundled_debts', value=bundle)
        logging.info(f"Sent bundled debt: {bundle}")

