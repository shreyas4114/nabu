from flask import Flask, request, jsonify
from order_matching.order_book import OrderBook, Order
from data_ingestion.kafka_producer import produce_data
from securitization.bundling import bundle_debts
import threading

# Initialize Flask app and the order book
app = Flask(__name__)
order_book = OrderBook()

@app.route("/orders", methods=["POST"])
def add_order():
    data = request.get_json()
    if not data:
        return jsonify({"error": "No input data provided"}), 400

    try:
        order_id = data.get("order_id")
        side = data.get("side")
        price = data.get("price")
        quantity = data.get("quantity")

        if not all([order_id, side, price, quantity]):
            return jsonify({"error": "Missing required order details"}), 400

        if side not in ["buy", "sell"]:
            return jsonify({"error": "Invalid side, must be 'buy' or 'sell'"}), 400

        order = Order(order_id=order_id, side=side, price=price, quantity=quantity)
        order_book.add_order(order)
        order_book.display_order_book()  # Debugging line to print the order book state
        return jsonify({"message": "Order added successfully", "order": vars(order)}), 201

    except Exception as e:
        return jsonify({"error": str(e)}), 500



@app.route("/orders", methods=["GET"])
def get_orders():
    try:
        buy_orders = [vars(order) for order in order_book.get_buy_orders()]
        sell_orders = [vars(order) for order in order_book.get_sell_orders()]
        return jsonify({"buy_orders": buy_orders, "sell_orders": sell_orders}), 200

    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/orders/match", methods=["POST"])
def match_orders():
    try:
        order_book.match_orders()
        transactions = order_book.get_transactions()
        if transactions:
            # No need to use vars() since transactions are already dictionaries
            return jsonify({"message": "Orders matched successfully", "transactions": transactions}), 200
        else:
            return jsonify({"message": "No matching orders found"}), 200

    except Exception as e:
        return jsonify({"error": str(e)}), 500



@app.route("/transactions", methods=["GET"])
def get_transactions():
    try:
        transactions = order_book.get_transactions()
        if transactions:
            return jsonify({"transactions": transactions}), 200  # No need for vars()
        else:
            return jsonify({"message": "No transactions available"}), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500



@app.route("/data/ingest", methods=["POST"])
def ingest_data():
    try:
        produce_data(order_book)
        return jsonify({"message": "Data ingestion triggered successfully"}), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/securitization/bundle", methods=["POST"])
def bundle_debt():
    try:
        result = bundle_debts()
        if "error" in result:
            return jsonify({"message": "Error bundling debts", "error": result["error"]}), 500
        return jsonify({"message": "Debts bundled successfully", "result": result}), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500


def start_background_tasks():
    threading.Thread(target=produce_data, args=(order_book,), daemon=True).start()
    threading.Thread(target=bundle_debts, daemon=True).start()

@app.route('/')
def index():
    return "Hello, World!"

if __name__ == "__main__":
    start_background_tasks()
    app.run(debug=True, host='0.0.0.0', port=8080)
