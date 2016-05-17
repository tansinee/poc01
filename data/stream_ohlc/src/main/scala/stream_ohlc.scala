import java.text.SimpleDateFormat
import java.util.{Date, Calendar}

import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.streaming._
import com.datastax.spark.connector._
import com.datastax.spark.connector.streaming._

object StreamingOhlc {
  val conf = new SparkConf()
              .setMaster("local[2]")
              .setAppName("OHLC")

  val ssc = new StreamingContext(conf, Seconds(20))

  def main(args: Array[String]) {
    val formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    def parseDateTime(t: String) = {
      val cal = Calendar.getInstance()
      cal.setTime(formatter.parse(t))
      cal
    }

    val transactions = ssc.socketTextStream("localhost", 9999)
                    .map(_.split(','))
                    .map(t => (t(0), parseDateTime(t(2)), t(1).toFloat))

    transactions.cache
    transactions.saveToCassandra("stock", "transactions2",
      SomeColumns("symbol", "tx_time", "price"))

    val ohlc = transactions.transform(rdd => {
      def truncateMin(cal: Calendar): Calendar = {
        val newCal: Calendar = cal.clone().asInstanceOf[Calendar]
        newCal.set(Calendar.SECOND, 0)
        newCal.set(Calendar.MILLISECOND, 0)
        newCal
      }

      val batchTransactions = rdd.map(t => new BatchTransaction(t._1, truncateMin(t._2), t._3, t._2))
      batchTransactions.keyBy(t => (t.symbol, t.batchTime))
        .mapValues(t => new OhlcValue(t.price, t.price, t.price, t.price, t.txTime, t.txTime))
        .reduceByKey((x, y) => new OhlcValue(
          high=math.max(x.high, y.high),
          low=math.min(x.low, y.low),
          open=if(x.openTime.before(y.openTime)) x.open else y.open,
          close=if(x.closeTime.after(y.closeTime)) x.close else y.close,
          openTime=if(x.openTime.before(y.openTime)) x.openTime else y.openTime,
          closeTime=if(x.closeTime.after(y.closeTime)) x.closeTime else y.closeTime
        ))
    })

    ohlc.map(x => (x._1._1, x._1._2, x._2.open, x._2.high, x._2.low, x._2.close, x._2.openTime, x._2.closeTime) )
        .saveToCassandra("stock", "ohlc_1_min2",
          SomeColumns("symbol", "batch_time", "open", "high", "low", "close", "open_time", "close_time"))

    ssc.start()
    ssc.awaitTermination()
  }


}

case class BatchTransaction(symbol: String, batchTime: Calendar, price: Float, txTime: Calendar)
case class OhlcValue(high: Float, low: Float, open: Float, close: Float, openTime: Calendar, closeTime: Calendar)