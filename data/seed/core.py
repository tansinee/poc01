import random
from datetime import datetime, timedelta

latest_prices = {}

def gen(at):
    tx_amount = random.randrange(20, 100, 1)
    txns = []
    for i in range(0, tx_amount):
        symbol = chr(random.randrange(65, 65 + 17, 1)) + \
                 chr(random.randrange(65, 65 + 17, 1)) + \
                 chr(random.randrange(65, 65 + 17, 1))
        milliseconds = random.randrange(0, 1000, 1)

        price = latest_prices.get(symbol, random.randrange(10, 200))
        price += [-1, 0, 1][random.randrange(0, 3)]
        price = 1 if price < 1 else price
        latest_prices[symbol] = price

        txns.append((symbol, price, at + timedelta(milliseconds=milliseconds)))

    return txns
