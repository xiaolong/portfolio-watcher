
import org.apache.spark.Logging
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.receiver.Receiver

import scala.io.Source

/**
  * Created by xcheng on 3/30/16.
  */
class GoogleStockPriceReceiver(val tickers: List[String]) extends Receiver[String](StorageLevel.MEMORY_AND_DISK_2) with Logging {

  def onStart() {
    // Start the thread that receives data over a connection
    new Thread("Google Stock Price Receiver") {
      override def run() { receive() }
    }.start()
  }

  def onStop() {
    // There is nothing much to do as the thread calling receive()
    // is designed to stop by itself if isStopped() returns false
  }

  /** Create a socket connection and receive data until receiver is stopped */
  private def receive() {

    try {
      while (! isStopped ){
        val url = "http://finance.google.com/finance/info?client=ig&q=" + tickers.mkString(",")

        logInfo(url) //

        val googleFinanceStream = Source.fromURL(url)
        val cleanJsonString = googleFinanceStream.getLines().mkString("").replace("//", "")

        store(cleanJsonString) //feed to spark

        googleFinanceStream.close()
      }//end while

      logInfo("Stopped receiving")
      restart("Trying to connect again")

    } catch {
      case e: java.net.ConnectException =>
        // restart if could not connect to server
        restart("Error connecting to endpoint", e)
      case t: Throwable =>
        // restart if there is any other error
        restart("Error receiving data", t)
    }

  }
}