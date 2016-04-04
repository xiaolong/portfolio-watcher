
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by xcheng on 3/30/16.
  */
object StockTrend {

  //keep the status of stock tickers, in (price, number_of_ups) pairs
  def updatePriceStat(newValue: Seq[(Double,Int)], oldValue: Option[(Double, Int)]) : Option[(Double, Int)] = {
    if (newValue.length > 0){
      val priceDiff = newValue(0)._1 - oldValue.getOrElse(newValue(0)._1, 0)._1
      Some((newValue(0)._1, if (priceDiff>0) 1 else 0))
    } else {
      oldValue
    }
  }


  def main(args: Array[String]) : Unit = {
    val sparkConf = new SparkConf().setAppName("StockTrend").setMaster("local[4]")
    val ssc = new StreamingContext(sparkConf, Seconds(1))

    Logger.getRootLogger.setLevel(Level.WARN)

    ssc.checkpoint("./tmp")

    val tickers = List("AAPL", "YHOO", "DIS", "SBUX")
    val lines = ssc.receiverStream(new GoogleStockPriceReceiver(tickers)) //initial D-Stream

    //helper function to parse json string to a list of maps
    def line_to_maps(line:String): List[Map[String, String]]={
      val parsed = scala.util.parsing.json.JSON.parseFull(line)
      val results = parsed match{
        case Some(lst : List[Map[String, String]]) => lst
        case _ => Nil
      }
      results
    }

    //transform lines into DStream of Stocks
    val stockStream = lines.map(line_to_maps).flatMap(lst => lst).map(Stock(_))

    //use random for testing
    import scala.util.Random

    //transform stock stream into a DStream of states (ticker, (current_price, number_of_ups_compared_to_previous))
    val stockState = stockStream.map(stock => (stock.t, (stock.l+Random.nextDouble(), -1)))
      .filter(state => state._1.isEmpty==false)
      .updateStateByKey(updatePriceStat _)


    //fold from left to right, i.e. process RDD for t-6, then RDD for t-5, then t-4, t-3, etc..
    def reduceFunc(left: (Double, Int), right: (Double, Int)) : (Double, Int) = {
      println("left "+left+" right "+right)
      (right._1, left._2 + right._2)
    }

    //Every 3 seconds, process RDDs in the past 6 seconds
    val stockTrend = stockState.reduceByKeyAndWindow(reduceFunc(_, _), Seconds(6), Seconds(3))


    stockState.print()
    stockTrend.print()

    ssc.start()
    ssc.awaitTermination()
    println("StockTrend")





  }
}
