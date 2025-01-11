from google.cloud import pubsub_v1
import json
import logging

logging.basicConfig(level=logging.INFO)

# def test_publish():
#     try:
#         publisher = pubsub_v1.PublisherClient()
#         topic_path = publisher.topic_path('nabu-446815', 'consumer_debts')
#         message = {"test": "message"}
#         future = publisher.publish(topic_path, data=str(message).encode("utf-8"))
#         logging.info(f"Message published: {future.result()}")
#     except Exception as e:
#         logging.error(f"Failed to publish message: {e}")

# test_publish()

def produce_data(order_book):
    """
    Produces the most recent transaction to the 'consumer_debts' Pub/Sub topic.
    """
    try:
        # Initialize Pub/Sub publisher
        publisher = pubsub_v1.PublisherClient()
        topic_path = publisher.topic_path('nabu-446815', 'consumer_debts')

        logging.info(f"Topic Path: {topic_path}")

        # Retrieve the most recent transaction
        transactions = order_book.get_transactions()
        logging.info(f"Transactions: {transactions}")

        if transactions:
            transaction = transactions[-1]  # Most recent transaction
            logging.info(f"Processing Transaction: {transaction}")

            # Fetch related buy and sell orders
            buy_order = next((o for o in order_book.bids if o.order_id == transaction['buy_order_id']), None)
            sell_order = next((o for o in order_book.asks if o.order_id == transaction['sell_order_id']), None)

            # Prepare data
            data = {
                "buy_order_id": transaction["buy_order_id"],
                "sell_order_id": transaction["sell_order_id"],
                "transaction_price": transaction["price"],
                "transaction_quantity": transaction["quantity"],
                "remaining_buy_quantity": buy_order.quantity if buy_order else 0,
                "remaining_sell_quantity": sell_order.quantity if sell_order else 0,
            }
            logging.info(f"Data to Publish: {data}")

            # Serialize data to JSON and publish
            data_json = json.dumps(data)
            future = publisher.publish(topic_path, data=data_json.encode('utf-8'))
            logging.info(f"Message published: {future.result()}")
        else:
            logging.info("No transactions found to produce.")
    except Exception as e:
        logging.error(f"Error in produce_data: {e}")
