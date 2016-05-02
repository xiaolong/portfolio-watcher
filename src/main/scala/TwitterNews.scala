/**
  * Created by xcheng on 4/6/16.
  */

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.twitter._


object TwitterNews {



  def main(args: Array[String]) : Unit = {

    // Twitter Authentication credentials
    System.setProperty("twitter4j.oauth.consumerKey", "C345rEYsrRV0KcU5MPU6o04Vk")
    System.setProperty("twitter4j.oauth.consumerSecret","8vqIjJxEgplIU4NLbJYPAeG1njeIgEi89YgBOaEmB1pgKzzxnU")
    System.setProperty("twitter4j.oauth.accessToken", "213591974-ePV3u8guvQiMPqdhl5Id0dhGowtBCuGIrC2kSAbk")
    System.setProperty("twitter4j.oauth.accessTokenSecret", "Awh9fZ6dUd91QbGrViT8c7TiLbck71TVvMcXX734Q57BM")

    // set up spark
    val sparkConf = new SparkConf().setAppName("TwitterNews").setMaster("local[4]")
    val ssc = new StreamingContext(sparkConf, Seconds(1))

    Logger.getRootLogger.setLevel(Level.WARN)

    val filter_strings = List("Apple", "Yahoo", "Google", "Facebook")

    val tstream = TwitterUtils.createStream(ssc, None, filter_strings)
    val en_stream = tstream.filter(_.getUser.getLang == "en")

    val statuses = en_stream.map(status=>status.getUser.getName+ " : " + status.getText).
      filter(status=>filter_strings.exists(status.contains(_)))

    //statuses.print()

    //statuses.saveAsTextFiles("file:///Users/xcheng/Downloads/testoutput", "txt");
    //statuses.saveAsHadoopFiles("hdfs://HadoopSystem-150s:8020/Spark_Twitter_out","txt");


    statuses.foreachRDD{
      rdd=> rdd.foreachPartition{

        partitionOfRecords =>

          // create connection here
          import java.sql.{Connection, DriverManager, ResultSet};
          //val driver = "org.postgresql.Driver"
          //Class.forName(driver) //load driver

          val conn = DriverManager.getConnection("jdbc:postgresql://localhost:5432/xcheng", "xcheng", "")
          val statement = conn.createStatement()

          try {
            partitionOfRecords.foreach {
              s => {
                println(s + "\n")

                val prep = conn.prepareStatement("INSERT INTO test.tweets (content) VALUES (?) ")

                prep.setString(1, s)
                prep.executeUpdate
              }

            }
          }
          finally {
            conn.close()
          }

      }
    }

    ssc.start()
    ssc.awaitTermination()

  }

}
