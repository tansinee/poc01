import os
from datetime import datetime, timedelta
from core import gen

def gen_period(start, stop):
    txns = []
    for x in range(0, int((stop - start).total_seconds())):
        txns += gen(start + timedelta(seconds=x))
    return txns


from cassandra.cluster import Cluster
from cassandra.query import BatchStatement

host = os.getenv("CASSANDRA_HOST", '10.0.2.15')
cluster = Cluster([host])
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
    if datetime(2016, 4, i).weekday() >= 5:
        continue
    print('Generating for day: ',i)
    txns = gen_period(datetime(2016, 4, i, 9, 30), datetime(2016, 4, i, 16, 30))
    write_to_cassandra(txns)

cluster.shutdown()
