from datetime import datetime
from pyspark.sql.functions import *

ctx = SQLContext(sc)

data = ctx.read.format('org.apache.spark.sql.cassandra').options(table='transactions', keyspace='stock', cluster='Test Cluster').load()

data2 = data.select('symbol', to_date(data.tx_time).alias('batch_time'), 'price')
min_data = data2.groupBy('symbol', 'batch_time').min()
min_data.show()




