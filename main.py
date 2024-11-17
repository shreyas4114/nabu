import logging
import threading
from order_matching.order_book import OrderBook, Order
from fix_protocol.fix_handler import start_fix_session
from data_ingestion.kafka_producer import produce_data
from risk_assessment.risk_model import assess_risk
from securitization.bundling import bundle_debts

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def main():
    logging.info("Initializing OrderBook...")
    order_book = OrderBook()  # Create an instance of OrderBook

    # Start FIX session
    logging.info("Starting FIX session...")
    fix_thread = threading.Thread(target=start_fix_session)
    fix_thread.start()

    # Produce sample data
    logging.info("Starting data producer...")
    producer_thread = threading.Thread(target=produce_data)
    producer_thread.start()

    # Securitize debts
    logging.info("Starting debt bundling...")
    bundling_thread = threading.Thread(target=bundle_debts)
    bundling_thread.start()

    # Add an order and match orders
    try:
        order = Order(order_id=1, side='buy', price=100, quantity=10)
        order_book.add_order(order)
        logging.info(f"Added order: {vars(order)}")

        # Match orders
        order_book.match_orders()
    except Exception as e:
        logging.error(f"Error occurred while adding or matching orders: {e}")

    # Wait for threads to complete (if needed)
    fix_thread.join()
    producer_thread.join()
    risk_thread.join()
    bundling_thread.join()

    logging.info("All threads have completed.")

if __name__ == "__main__":
    main()
