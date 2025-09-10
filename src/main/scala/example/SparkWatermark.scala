/** SparkWatermark.scala
  */

package example

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.DataTypes
import org.apache.spark.sql.streaming.OutputMode
import java.sql.Timestamp
import org.apache.spark.sql.Encoders

import org.apache.spark.sql.functions.{col, window}

object SparkWatermark {

  val spark = SparkSession.builder
    .appName("SparkWatermark")
    .master("local[*]")
    .getOrCreate
  spark.sparkContext.setLogLevel("ERROR")
  spark.conf.set(
    "spark.sql.streaming.checkpointLocation",
    "/Users/millodev/Documents/kafka"
  )

  def main(args: Array[String]): Unit = {
    val streaming = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "stream-person")
      .load
      .selectExpr(
        "CAST(key AS STRING) AS key",
        "CAST(value AS STRING) AS value",
        "CAST(timestamp AS TIMESTAMP) as timestamp"
      )

    val windowTimestamp = streaming
      .groupBy(
        window(col("timestamp"), "1 day", "1 hour")
      )
      .count()

    // val out =
    windowTimestamp.writeStream
      .format("Console")
      .outputMode(OutputMode.Complete)
      .option("truncate", "false")
      .start()
      .awaitTermination()
  }

}
