from kafka import KafkaProducer
import json
import logging
from order_matching.order_book import OrderBook, Order  # Ensure this import works based on your project structure
def produce_data(order_book):
    """
    Produces a single order book transaction (buy price, sell price, quantity, and remaining quantity) 
    to the 'consumer_debts' Kafka topic.
    """
    try:
        producer = KafkaProducer(
            bootstrap_servers='127.0.0.1:9092',
            request_timeout_ms=20000,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )

        transactions = order_book.get_transactions()
        if transactions:
            transaction = transactions[-1]  # Taking the last (most recent) transaction

            buy_order = next(
                (o for o in order_book.bids if o.order_id == transaction['buy_order_id']), None
            )
            sell_order = next(
                (o for o in order_book.asks if o.order_id == transaction['sell_order_id']), None
            )

            data = {
                "buy_order_id": transaction["buy_order_id"],
                "sell_order_id": transaction["sell_order_id"],
                "transaction_price": transaction["price"],
                "transaction_quantity": transaction["quantity"],
                "remaining_buy_quantity": buy_order.quantity if buy_order else 0,
                "remaining_sell_quantity": sell_order.quantity if sell_order else 0,
            }

            # Send data to Kafka
            producer.send('consumer_debts', value=data)
            logging.info(f"Produced data to 'consumer_debts': {data}")

            producer.flush()
        else:
            logging.info("No transactions found to produce.")

    except Exception as e:
        logging.error(f"Error producing data: {e}")
