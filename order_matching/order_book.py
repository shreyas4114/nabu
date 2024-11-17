# order_book.py
class Order:
    def __init__(self, order_id, side, price, quantity):
        self.order_id = order_id
        self.side = side
        self.price = price
        self.quantity = quantity

class OrderBook:
    def __init__(self):
        self.bids = []
        self.asks = []

    def add_order(self, order):
        if order.side == 'buy':
            self.bids.append(order)
            self.bids.sort(key=lambda x: x.price, reverse=True)
        else:
            self.asks.append(order)
            self.asks.sort(key=lambda x: x.price)

    def match_orders(self):
        while self.bids and self.asks and self.bids[0].price >= self.asks[0].price:
            bid = self.bids[0]
            ask = self.asks[0]
            quantity = min(bid.quantity, ask.quantity)
            print(f"Trade executed: {quantity} units at {ask.price}")
            # Adjust quantities or remove orders
            if bid.quantity > quantity:
                bid.quantity -= quantity
            else:
                self.bids.pop(0)
            if ask.quantity > quantity:
                ask.quantity -= quantity
            else:
                self.asks.pop(0)
