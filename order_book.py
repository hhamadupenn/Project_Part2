from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from datetime import datetime

from models import Base, Order
engine = create_engine('sqlite:///orders.db')
Base.metadata.bind = engine
DBSession = sessionmaker(bind=engine)
session = DBSession()

def process_order(order):
    #Your code here
    fields = ['sender_pk','receiver_pk','buy_currency','sell_currency','buy_amount','sell_amount']
    order_obj = Order(**{f:order[f] for f in fields})

    session.add(order_obj)
    session.commit()

    orders = session.query(Order).all() #Get all orders
    for order in orders:
        if order.filled == None and order.buy_currency == order_obj.sell_currency and order.sell_currency == order_obj.buy_currency and (order.sell_amount / order.buy_amount) >= (order_obj.buy_amount / order_obj.sell_amount):
            time_stamp = datetime.utcnow
            order.filled = time_stamp
            order_obj.filled = time_stamp
            order_obj.counterparty_id = order.id
            order.counterparty_id = order_obj.id
            session.commit()

            # create new order
            new_order_obj = {}
            if order.sell_amount < order_obj.buy_amount:
                new_order_obj['creator_id'] = order_obj.id
                new_order_obj['sender_pk'] = order_obj.sender_pk
                new_order_obj['receiver_pk'] = order_obj.receiver_pk
                new_order_obj['buy_currency'] = order_obj.buy_currency
                new_order_obj['sell_currency'] = order_obj.sell_currency
                

                

            
            return


