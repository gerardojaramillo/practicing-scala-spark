/**
 * StreamingSparkDemo.scala
 */

package example

import org.apache.spark.streaming.StreamingContext
import org.apache.spark.sql.SparkSession

import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Row
import org.apache.spark.SparkConf
import org.apache.spark.sql.streaming.Trigger

import scala.concurrent.duration._
import org.apache.spark.sql.expressions.Window

object StreamingSparkDemo {

  val checkpointLoc = "/Users/millodev/Downloads/temp"

  def main(args: Array[String]): Unit = {

    val config = new SparkConf()
      .setMaster("local[*]")
      .setAppName("StreamingSparkDemo")
      .set("spark.sql.shuffle.partitions", "10")

    val spark = SparkSession.builder().config(config).getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    import spark.implicits._

    val df = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 1234)
      .load()
      .selectExpr("CAST(value AS STRING)")
      .as[String]
      .map { line =>
        {
          val split = line.split(",").map(_.trim)
          (split(0), split(1), split(2), split(3), split(4), split(5))
        }
      }
      .toDF("name", "pat", "mat", "birthday", "gender", "country")

    val groupByGender = df
      .groupBy(col("gender"))
      .agg(count(col("*")).as("counter"))
      .withColumn(
        "percentage",
        round(
          col("counter") / sum(col("counter")).over(Window.partitionBy()) * 100,
          2))
      .orderBy(col("counter"))

    val output = groupByGender.writeStream
      .format("console")
      .outputMode(OutputMode.Complete())
      .option("truncate", "false")
      .option("checkpointLocation", checkpointLoc)
      .trigger(Trigger.ProcessingTime(3.second))
      .start()
    output.awaitTermination()
  }

}
