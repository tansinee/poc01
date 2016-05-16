from datetime import datetime

from pyspark import SparkConf, SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import functions as F
from pyspark.sql import Row, SQLContext
from pyspark.sql.types import TimestampType


conf = SparkConf() \
        .setAppName('OHLC') \
        .setMaster('local[4]') \
        .set('spark.cassandra.connection.host', '10.0.2.15')

sc = SparkContext(conf=conf)
ssc = StreamingContext(sc, 30)

transactions = ssc.socketTextStream('localhost', 9999)

def get_sql_context_instance(sparkContext):
    if ('sparkSessionSingletonInstance' not in globals()):
        globals()['sparkSessionSingletonInstance'] = SQLContext(sparkContext)
    return globals()['sparkSessionSingletonInstance']

truncate_min = F.udf(lambda dt: datetime(dt.year, dt.month, dt.day, dt.hour, dt.minute), TimestampType())

def reduce_to_ohlc(time, rdd):
    row_rdd = rdd.map(lambda row: row.split(',')) \
                 .filter(lambda row: len(row) == 3) \
                 .map(lambda row: Row(
                       symbol=row[0],
                       tx_time=datetime.strptime(row[2], '%Y-%m-%d %H:%M:%S.%f'),
                       price=float(row[1])
                 ))
    sql_context = get_sql_context_instance(rdd.context)
    data = sql_context.createDataFrame(row_rdd)
    data.cache()
    data.write.format('org.apache.spark.sql.cassandra') \
            .options(table='transactions2', keyspace='stock', cluster='Test Cluster') \
            .mode('append') \
            .save()

    ohlc = data.select('symbol', truncate_min(data.tx_time).alias('batch_time'), 'price', 'tx_time') \
                .orderBy('tx_time') \
                .groupBy('symbol', 'batch_time') \
                .agg(
                   F.first(data.price).alias('open'),
                   F.max(data.price).alias('high'),
                   F.min(data.price).alias('low'),
                   F.last(data.price).alias('close'),
                   F.first(data.tx_time).alias('open_time'),
                   F.last(data.tx_time).alias('close_time')
                )

    existing_ohlc = sql_context.read.format('org.apache.spark.sql.cassandra') \
            .options(table='ohlc_1_min2', keyspace='stock', cluster='Test Cluster') \
            .load() \
            .select('symbol', 'batch_time', 'open', 'open_time', 'high', 'low', 'close', 'close_time')

    merged_ohlc = ohlc.join(existing_ohlc,
                             (ohlc.symbol == existing_ohlc.symbol) &
                             (ohlc.batch_time == existing_ohlc.batch_time),
                             'left'
                           )

    merged_ohlc = merged_ohlc.select(
        ohlc.symbol.alias('symbol'),
        ohlc.batch_time.alias('batch_time'),
        F.when(existing_ohlc.open_time < ohlc.open_time, existing_ohlc.open).otherwise(ohlc.open).alias('open'),
        F.when(existing_ohlc.open_time < ohlc.open_time, existing_ohlc.open_time).otherwise(ohlc.open_time).alias('open_time'),
        F.when(existing_ohlc.close_time > ohlc.close_time, existing_ohlc.close).otherwise(ohlc.close).alias('close'),
        F.when(existing_ohlc.close_time > ohlc.close_time, existing_ohlc.close_time).otherwise(ohlc.close_time).alias('close_time'),
        F.when(existing_ohlc.low < ohlc.low, existing_ohlc.low).otherwise(ohlc.low).alias('low'),
        F.when(existing_ohlc.high > ohlc.high, existing_ohlc.high).otherwise(ohlc.high).alias('high')
    )
    merged_ohlc.write.format('org.apache.spark.sql.cassandra') \
                .options(table='ohlc_1_min2', keyspace='stock', cluster='Test Cluster') \
                .mode('append') \
                .save()


transactions.foreachRDD(reduce_to_ohlc)
ssc.start()
ssc.awaitTermination()
