from datetime import datetime, timedelta
import random
from itertools import chain

latest_prices = {}

def gen(at):
    tx_amount = random.randrange(20, 100, 1)
    for i in xrange(0, tx_amount):
        symbol = chr(random.randrange(65, 65 + 17, 1)) + \
                 chr(random.randrange(65, 65 + 17, 1)) + \
                 chr(random.randrange(65, 65 + 17, 1))
        milliseconds = random.randrange(0, 1000, 1)

        price = latest_prices.get(symbol, random.randrange(10, 200))
        price += [-1, 0, 1][random.randrange(0, 3)]
        price = 1 if price < 1 else price
        latest_prices[symbol] = price

        yield (symbol, price, at + timedelta(milliseconds=milliseconds))

def gen_period(start, stop):
    return chain.from_iterable([
        gen(start + timedelta(seconds=x))
        for x in range(0, int((stop - start).total_seconds()))
    ])

def run():
    dt = datetime.now()
    dt = dt - timedelta(microseconds=dt.microsecond)

    txns = gen(dt)
    for item in txns:
        print item

def run_30_days():
    txns = chain.from_iterable([
        gen_period(datetime(2016, 4, day, 9, 30), datetime(2016, 4, day, 16, 30))
        for day in range(1, 31)
        if datetime(2016, 4, day).weekday() < 5
    ])

    for item in txns:
        print item
