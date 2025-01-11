from google.cloud import pubsub_v1
import json
import logging

def bundle_debts():
    """
    Processes a batch of messages from the 'consumer_debts-sub' subscription.
    """
    try:
        # Initialize Pub/Sub subscriber
        subscriber = pubsub_v1.SubscriberClient()
        subscription_path = subscriber.subscription_path('nabu-446815', 'consumer_debts-sub')
        logging.info(f"Subscription Path: {subscription_path}")

        # Pull messages (batch mode)
        response = subscriber.pull(
            subscription=subscription_path,
            max_messages=10,  # Adjust the number as needed
            timeout=30  # Timeout in seconds
        )

        # Process each message
        publisher = pubsub_v1.PublisherClient()
        topic_path = publisher.topic_path('nabu-446815', 'bundled_debts')

        bundled_debts = []
        for received_message in response.received_messages:
            logging.info(f"Received debt message: {received_message.message.data}")
            debt = json.loads(received_message.message.data)

            # Validate debt data
            if (
                'transaction_price' in debt
                and 'transaction_quantity' in debt
                and debt['transaction_price'] > 0
                and debt['transaction_quantity'] > 0
            ):
                bundled_debt = {
                    "total_transaction_value": round(debt['transaction_price'] * debt['transaction_quantity'], 2),
                    "remaining_buy_quantity": debt.get("remaining_buy_quantity", 0),
                    "remaining_sell_quantity": debt.get("remaining_sell_quantity", 0),
                }
                bundled_debts.append(bundled_debt)

                # Publish bundled debt
                publisher.publish(topic_path, data=json.dumps(bundled_debt).encode('utf-8'))
                logging.info(f"Published bundled debt: {bundled_debt}")
            else:
                logging.warning(f"Invalid debt data: {debt}")

            # Acknowledge the message
            subscriber.acknowledge(subscription=subscription_path, ack_ids=[received_message.ack_id])

        return bundled_debts  # Return the processed debts
    except Exception as e:
        logging.error(f"Error during bundling: {e}")
        return {"error": str(e)}
