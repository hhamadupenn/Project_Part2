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

    orders = session.query(Order).filter(Order.filled == None).all() #Get all orders
    for existing_order in orders:
        if existing_order.filled == None and existing_order.buy_currency == order_obj.sell_currency and existing_order.sell_currency == order_obj.buy_currency and (existing_order.sell_amount / existing_order.buy_amount) >= (order_obj.buy_amount / order_obj.sell_amount):
            time_stamp = datetime.now()
            existing_order.filled = time_stamp
            order_obj.filled = time_stamp
            order_obj.counterparty_id = existing_order.id
            existing_order.counterparty_id = order_obj.id
            session.commit()

            # create new order
            new_order_obj = {}
            fields.append('creator_id')
            if existing_order.buy_amount < order_obj.sell_amount and (order.buy_amount/order.creator.buy_amount) <= (order.sell_amount/order.creator.sell_amount):
                new_order_obj = order.copy()
                new_order_obj['creator_id'] = order_obj.id
                new_order_obj['sender_pk'] = order_obj.sender_pk
                new_order_obj['receiver_pk'] = order_obj.receiver_pk
                new_order_obj['buy_currency'] = order_obj.buy_currency
                new_order_obj['sell_currency'] = order_obj.sell_currency
                new_order_obj['buy_amount'] = (order_obj.buy_amount / order_obj.sell_amount) * (order_obj.sell_amount - existing_order.buy_amount) 
                new_order_obj['sell_amount'] = (order_obj.sell_amount - existing_order.buy_amount)
                child_order = Order(**{f:new_order_obj[f] for f in fields})
                session.add(child_order)
                session.commit()
            elif existing_order.buy_amount > order_obj.sell_amount:
                new_order_obj['creator_id'] = existing_order.id
                new_order_obj['sender_pk'] = existing_order.sender_pk
                new_order_obj['receiver_pk'] = existing_order.receiver_pk
                new_order_obj['buy_currency'] = existing_order.buy_currency
                new_order_obj['sell_currency'] = existing_order.sell_currency
                new_order_obj['buy_amount'] = (existing_order.buy_amount - order_obj.sell_amount)
                new_order_obj['sell_amount'] = (existing_order.sell_amount / existing_order.buy_amount) * (existing_order.buy_amount - existing_order.sell_amount)
                child_order = Order(**{f:new_order_obj[f] for f in fields})
                session.add(child_order)
                session.commit()

            return


