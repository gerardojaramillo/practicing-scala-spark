/**
 * SparkSocket.scala
 * @author
 *   Gerardo Jaramillo
 */

package example

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.Encoders

import org.apache.log4j.{Level, Logger}

import java.sql.Timestamp

object SparkSocket {

  Logger
    .getLogger("org.apache.spark")
    .setLevel(Level.ERROR)

  val spark =
    SparkSession
      .builder()
      .appName("SparkSocket")
      .master("local[*]")
      .getOrCreate()
  spark.sparkContext.setLogLevel(Level.ERROR.toString)

  def main(args: Array[String]): Unit = {
    val streaming = spark.readStream
      .format("socket")
      .option("host", "127.0.0.1")
      .option("port", 1234)
      .load()

    val df = streaming
      .selectExpr(
        "CAST(value AS STRING) AS value",
        "CAST(timestamp AS TIMESTAMP) AS timestamp"
      )
      .as[(String, Timestamp)](
        Encoders.tuple(Encoders.STRING, Encoders.TIMESTAMP)
      )

    val query = df.writeStream
      .format("console")
      .outputMode(OutputMode.Append)
      .option("truncate", "false")

    query.start().awaitTermination()

  }

}
