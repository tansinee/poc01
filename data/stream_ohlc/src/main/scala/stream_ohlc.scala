import java.text.SimpleDateFormat
import java.util.Date

import org.apache.spark._
import org.apache.spark.streaming._

object StreamingOhlc {
  val conf = new SparkConf()
              .setMaster("local[2]")
              .setAppName("OHLC")

  val ssc = new StreamingContext(conf, Seconds(20))

  def main(args: Array[String]) {
    val formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    def parseDateTime(t: String) = formatter.parse(t)

    val transactions = ssc.socketTextStream("localhost", 9999)
                    .map(_.split(','))
                    .map(t => (t(0), parseDateTime(t(2)), t(1).toFloat))

    transactions.print()

    ssc.start()
    ssc.awaitTermination()
  }
}
