from datetime import datetime

from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext
from pyspark.sql import functions as F
from pyspark.sql.functions import to_date

conf = SparkConf()\
        .setAppName('batch ohlc') \
        .setMaster('local')
sc = SparkContext(conf=conf)
ctx = SQLContext(sc)

data = ctx.read.format('org.apache.spark.sql.cassandra') \
            .options(table='transactions', keyspace='stock', cluster='Test Cluster') \
            .load()

daily_ohlc = data.select('symbol', to_date(data.tx_time).alias('batch_time'), 'price') \
                 .groupBy('symbol', 'batch_time') \
                 .agg(
                    F.first(data.price).alias('open'),
                    F.max(data.price).alias('high'),
                    F.min(data.price).alias('low'),
                    F.last(data.price).alias('close')
                 )

daily_ohlc.show()
