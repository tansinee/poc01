from datetime import datetime, timedelta
import random
from itertools import chain

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

def gen_period(start, stop):
    txns = []
    for x in range(0, int((stop - start).total_seconds())):
        txns += gen(start + timedelta(seconds=x))
    return txns


from cassandra.cluster import Cluster
from cassandra.query import BatchStatement

cluster = Cluster(['10.0.2.15'])
session = cluster.connect('stock')

create_tx_stmt = session.prepare(
    'INSERT INTO transactions (symbol, price, tx_time) VALUES (?, ?, ?)'
)
batch_size = 1000
def write_to_cassandra(transactions):
    tx_amount = len(transactions)
    print('Start write to cassandra ', tx_amount, 'records')
    t1 = datetime.now()
    print(t1)

    for i in range(0, int(tx_amount / batch_size) + 1):
        batch = BatchStatement()
        start = i * batch_size
        end = (i + 1) * batch_size
        end = len(txns) if end > tx_amount else end
        for item in transactions[start:end]:
            batch.add(create_tx_stmt, item)
        session.execute(batch)

    total_time = (datetime.now() - t1).total_seconds()
    print('Written ', total_time, 's')
    print('Speed', tx_amount / total_time, 'records')


for i in range(1, 31):
    print('Generating for day: ',i)
    txns = gen_period(datetime(2016, 4, i, 9, 30), datetime(2016, 4, i, 16, 30))
    write_to_cassandra(txns)

cluster.shutdown()
