import java.text.SimpleDateFormat
import java.util.{Date, Calendar}

import org.apache.spark._
import org.apache.spark.streaming._
import com.datastax.spark.connector._
import com.datastax.spark.connector.streaming._

object StreamingOhlc {
  val conf = new SparkConf()
              .setMaster("local[2]")
              .setAppName("OHLC")

  val ssc = new StreamingContext(conf, Seconds(20))


  def refreshOhlc(key: (String, Calendar), value: Option[OhlcValue], state: State[OhlcValue]) = {
    var newOhlcValue: OhlcValue = null
    if (state.exists) {
      val existingState: OhlcValue = state.get
      if (value.isDefined) {
        newOhlcValue = reduceOhlc(value.get, existingState)
        state.update(newOhlcValue)
      } else {
        newOhlcValue = existingState
        val expiredTime = Calendar.getInstance
        expiredTime.roll(Calendar.MINUTE, -3)
        if (key._2.before(expiredTime))
          state.remove
      }
    } else {
      newOhlcValue = value.get
      state.update(newOhlcValue)
    }

    (key, newOhlcValue)
  }

  val refreshOhlcSpec = StateSpec.function(refreshOhlc _)


  def main(args: Array[String]) {
    ssc.checkpoint("_checkpoints")
    
    //TODO: Thread-safe aware
    val formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS")
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

    val ohlcStream = transactions.transform(rdd => {
      def truncateMin(cal: Calendar): Calendar = {
        val newCal: Calendar = cal.clone().asInstanceOf[Calendar]
        newCal.set(Calendar.SECOND, 0)
        newCal.set(Calendar.MILLISECOND, 0)
        newCal
      }

      rdd.map(t => new BatchTransaction(t._1, truncateMin(t._2), t._3, t._2))
        .keyBy(t => (t.symbol, t.batchTime))
        .mapValues(t => new OhlcValue(t.price, t.price, t.price, t.price, t.txTime, t.txTime))
        .reduceByKey(reduceOhlc _)
    }).mapWithState(refreshOhlcSpec)

    ohlcStream.map(x => (x._1._1, x._1._2, x._2.open, x._2.high, x._2.low, x._2.close, x._2.openTime, x._2.closeTime) )
        .saveToCassandra("stock", "ohlc_1_min2",
          SomeColumns("symbol", "batch_time", "open", "high", "low", "close", "open_time", "close_time"))

    ssc.start()
    ssc.awaitTermination()
  }

  def reduceOhlc(x: OhlcValue, y: OhlcValue): OhlcValue = {
    new OhlcValue(
      high=math.max(x.high, y.high),
      low=math.min(x.low, y.low),
      open=if(x.openTime.before(y.openTime)) x.open else y.open,
      close=if(x.closeTime.after(y.closeTime)) x.close else y.close,
      openTime=if(x.openTime.before(y.openTime)) x.openTime else y.openTime,
      closeTime=if(x.closeTime.after(y.closeTime)) x.closeTime else y.closeTime
    )
  }

}

case class BatchTransaction(symbol: String, batchTime: Calendar, price: Float, txTime: Calendar)
case class OhlcValue(high: Float, low: Float, open: Float, close: Float, openTime: Calendar, closeTime: Calendar)