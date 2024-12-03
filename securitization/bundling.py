from kafka import KafkaProducer, KafkaConsumer
import json
import logging
def bundle_debts():
    """
    Consumes processed debts from Kafka, applies bundling logic, and produces bundled debts.
    """
    try:
        consumer = KafkaConsumer(
            'consumer_debts',  
            bootstrap_servers='127.0.0.1:9092',
            group_id='order-bundling-group',
            auto_offset_reset='earliest',
            enable_auto_commit=True,
            consumer_timeout_ms=10000
        )

        producer = KafkaProducer(
            bootstrap_servers='127.0.0.1:9092',
            request_timeout_ms=20000,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )

        for msg in consumer:
            logging.info(f"Received debt message: {msg.value}")

            debt = json.loads(msg.value)

            # Check for valid debt data
            if 'transaction_price' in debt and 'transaction_quantity' in debt and debt['transaction_price'] > 0 and debt['transaction_quantity'] > 0:
                bundled_debt = {
                    "total_transaction_value": round(debt['transaction_price'] * debt['transaction_quantity'], 2),
                    "remaining_buy_quantity": debt.get("remaining_buy_quantity", 0),
                    "remaining_sell_quantity": debt.get("remaining_sell_quantity", 0)
                }

                # Send bundled debt to the next Kafka topic
                producer.send('bundled_debts', value=bundled_debt)
                logging.info(f"Sent bundled debt: {bundled_debt}")
            else:
                logging.warning(f"Received invalid debt data: {debt}")

    except Exception as e:
        logging.error(f"Error during bundling: {e}")
