# kafka_producer.py
from kafka import KafkaProducer
import json

def produce_data():
    producer = KafkaProducer(bootstrap_servers='127.0.0.1:9092',
                         request_timeout_ms=20000,
                         value_serializer=lambda v: json.dumps(v).encode('utf-8'))


    data = {"id": 1, "amount": 1000, "credit_score": 700}
    producer.send('consumer_debts', value=data)
    producer.flush()
