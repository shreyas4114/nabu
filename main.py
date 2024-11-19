import logging
import threading
from order_matching.order_book import OrderBook, Order
from fix_protocol.fix_handler import start_fix_session
from data_ingestion.kafka_producer import produce_data
from securitization.bundling import bundle_debts

# Configure logging to show only INFO level messages and above
logging.basicConfig(
    level=logging.INFO,  # Only INFO, WARNING, ERROR, and CRITICAL logs will be displayed
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# Suppress all logs from external libraries like Kafka and QuickFIX to prevent clutter
logging.getLogger("kafka").setLevel(logging.CRITICAL)
logging.getLogger("quickfix").setLevel(logging.CRITICAL)

def initialize_order_book():
    """
    Initialize and populate the order book for testing.
    """
    logging.info("Initializing OrderBook...")
    order_book = OrderBook()

    try:
        # Example order addition
        buy_order = Order(order_id=1, side='buy', price=100, quantity=10)
        sell_order = Order(order_id=2, side='sell', price=95, quantity=5)

        order_book.add_order(buy_order)
        order_book.add_order(sell_order)

        logging.info(f"Added buy order: {vars(buy_order)}")
        logging.info(f"Added sell order: {vars(sell_order)}")

        # Match orders
        logging.info("Attempting to match orders...")
        order_book.match_orders()

        # Log transactions
        transactions = order_book.get_transactions()
        if transactions:
            for transaction in transactions:
                logging.info(f"Transaction executed: {transaction}")
        else:
            logging.info("No transactions were executed.")

    except Exception as e:
        logging.error(f"Error while managing orders: {e}")
        raise  # Re-raise the exception to ensure the program stops if order management fails

    return order_book

def start_threads():
    """
    Start separate threads for FIX session, data production, and debt bundling.
    """
    logging.info("Starting threads for FIX session, data production, and debt bundling...")
    
    try:
        # Start FIX session thread
        fix_thread = threading.Thread(target=start_fix_session, name="FIXSessionThread")
        fix_thread.start()

        # Start data producer thread
        producer_thread = threading.Thread(target=produce_data, name="DataProducerThread")
        producer_thread.start()

        # Start debt bundling thread
        bundling_thread = threading.Thread(target=bundle_debts, name="DebtBundlingThread")
        bundling_thread.start()

        return fix_thread, producer_thread, bundling_thread
    except Exception as e:
        logging.error(f"Error while starting threads: {e}")
        raise  # Re-raise the exception to ensure the program stops if thread initialization fails

def main():
    """
    Main entry point for the application.
    """
    logging.info("Application starting...")

    try:
        # Initialize the order book
        order_book = initialize_order_book()

        # Start threads for other tasks
        fix_thread, producer_thread, bundling_thread = start_threads()

        # Wait for threads to complete
        fix_thread.join()
        producer_thread.join()
        bundling_thread.join()

        logging.info("All operations completed successfully.")

    except Exception as e:
        logging.error(f"Error during main execution: {e}")
        logging.info("Application terminated with errors.")

if __name__ == "__main__":
    main()
