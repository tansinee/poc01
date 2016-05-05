from datetime import datetime

from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext
from pyspark.sql import functions as F
from pyspark.sql.types import TimestampType

conf = SparkConf()\
        .setAppName('batch ohlc') \
        .setMaster('local')
sc = SparkContext(conf=conf)
ctx = SQLContext(sc)

data = ctx.read.format('org.apache.spark.sql.cassandra') \
            .options(table='transactions', keyspace='stock', cluster='Test Cluster') \
            .load() \
            .cache()

def transform_to_ohlc(df, time_calc_fn, table_name):
    ohlc = df.select('symbol', time_calc_fn(data.tx_time).alias('batch_time'), 'price') \
                .groupBy('symbol', 'batch_time') \
                .agg(
                   F.first(data.price).alias('open'),
                   F.max(data.price).alias('high'),
                   F.min(data.price).alias('low'),
                   F.last(data.price).alias('close')
                )
    ohlc.write.format('org.apache.spark.sql.cassandra') \
                .options(table=table_name, keyspace='stock', cluster='Test Cluster') \
                .save()

transform_to_ohlc(data, F.to_date, 'ohlc_1_day')

truncate_hour = F.udf(lambda dt: datetime(dt.year, dt.month, dt.day, dt.hour), TimestampType())
transform_to_ohlc(data, truncate_hour, 'ohlc_1_hour')

truncate_min = F.udf(lambda dt: datetime(dt.year, dt.month, dt.day, dt.hour, dt.minute), TimestampType())
transform_to_ohlc(data, truncate_min, 'ohlc_1_min')
