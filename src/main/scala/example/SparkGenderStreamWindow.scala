/** SparkGenderStreamWindow.scala
  */

package example

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Encoders
import org.apache.spark.sql.catalyst.expressions.Encode
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.window
import org.apache.spark.sql.types._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.DataTypes
import java.time.Instant
import java.sql.Timestamp

object SparkGenderStreamWindow {

  /**
  val schema = new StructType()
    .add(StructField("name", DataTypes.StringType, true))
    .add(StructField("paternal", DataTypes.StringType, true))
    .add(StructField("maternal", DataTypes.StringType, true))
    .add(StructField("birthday", DataTypes.StringType, true))
    .add(StructField("gender", DataTypes.StringType, true))
    .add(StructField("country", DataTypes.StringType, true))
    */

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .appName("SparkGenderStreamWindow")
      .master("local[*]")
      .getOrCreate
    spark.sparkContext.setLogLevel("ERROR")

    val streaming = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      //.option("subscribe", "stream-person")
      .option("subscribe", "atm-oper-src")
      // .option("delimiter", ",")
      // .option("startingOffsets", "earliest")
      .load

    val streamingParsed =
      streaming
        .selectExpr(
          "CAST(value AS STRING)",
          "CAST(timestamp AS TIMESTAMP)"
        )
        .as[(String, Timestamp)](
          Encoders.tuple(Encoders.STRING, Encoders.TIMESTAMP)
        )

    val streamWindow = streamingParsed
      .withWatermark("timestamp", "1 hour")
      .groupBy(
        col("timestamp"),
        window(col("timestamp"), "1 hour", "10 minutes")
      )
      .count

    streamWindow.writeStream
      .outputMode(OutputMode.Complete)
      .format("Console")
      .option("checkpointLocation", "/Users/millodev/Documents/kafka")
      .option("truncate", "false")
      .start
      .awaitTermination
  }

}
