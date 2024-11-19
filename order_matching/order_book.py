class Order:
    def __init__(self, order_id, side, price, quantity):
        self.order_id = order_id
        self.side = side
        self.price = price
        self.quantity = quantity

    def __repr__(self):
        return f"Order({self.order_id}, {self.side}, {self.price}, {self.quantity})"


class OrderBook:
    def __init__(self):
        self.bids = []  # Buy orders
        self.asks = []  # Sell orders
        self.transactions = []  # To log executed trades

    def _sort_orders(self):
        """
        Sort orders: Bids by descending price, Asks by ascending price.
        """
        self.bids.sort(key=lambda x: x.price, reverse=True)
        self.asks.sort(key=lambda x: x.price)

    def add_order(self, order):
        """
        Add an order to the order book and sort it.
        """
        if order.side == 'buy':
            self.bids.append(order)
        elif order.side == 'sell':
            self.asks.append(order)
        else:
            raise ValueError("Order side must be 'buy' or 'sell'.")
        self._sort_orders()

    def match_orders(self):
        """
        Match buy and sell orders based on price and execute trades.
        """
        while self.bids and self.asks and self.bids[0].price >= self.asks[0].price:
            bid = self.bids[0]
            ask = self.asks[0]
            
            # Calculate the transaction quantity
            quantity = min(bid.quantity, ask.quantity)
            transaction_price = ask.price

            # Log the transaction
            transaction = {
                'buy_order_id': bid.order_id,
                'sell_order_id': ask.order_id,
                'price': transaction_price,
                'quantity': quantity,
            }
            self.transactions.append(transaction)
            print(f"Trade executed: {transaction}")

            # Adjust remaining quantities or remove orders
            if bid.quantity > quantity:
                bid.quantity -= quantity
            else:
                self.bids.pop(0)

            if ask.quantity > quantity:
                ask.quantity -= quantity
            else:
                self.asks.pop(0)

    def get_transactions(self):
        """
        Retrieve the list of executed transactions.
        """
        return self.transactions

    def display_order_book(self):
        """
        Display the current state of the order book.
        """
        print("\nOrder Book:")
        print("Bids (Buy Orders):", self.bids)
        print("Asks (Sell Orders):", self.asks)

