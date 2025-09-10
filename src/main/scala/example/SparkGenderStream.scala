/** SparkGenderStream.scala
  */

package example

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.Encoders
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.OutputMode

object SparkGenderStream {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder
      .appName("SparkGenderStream")
      .master("local[*]")
      .getOrCreate
    spark.sparkContext.setLogLevel("ERROR")

    val streaming = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "stream-person")
      .option("includeHeaders", "true")
      .load

    val query = streaming
      .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
      .as[(String, String)](Encoders.tuple(Encoders.STRING, Encoders.STRING))
      .writeStream
      .outputMode(OutputMode.Append)
      .format("Console")
      .option("checkpointLocation", "/Users/millodev/Documents")
      .start
    query.awaitTermination

  }

}
