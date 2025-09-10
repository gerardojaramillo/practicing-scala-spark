/**
 * SparkDemo.scala
 */

package example

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.DataTypes._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import org.apache.log4j.{Level, Logger}

object SparkDemo {

  Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)

  val path = getClass()
    .getResource("/demo.csv")
    .getPath()

  def main(args: Array[String]): Unit = {

    val spark =
      SparkSession
        .builder()
        .appName("SparkDemo")
        .master("local[*]")
        .getOrCreate()
    spark.sparkContext.setLogLevel(Level.ERROR.toString)

    val schema =
      new StructType()
        .add(StructField("name", StringType, false))
        .add(StructField("paternal", StringType, false))
        .add(StructField("maternal", StringType, false))
        .add(StructField("birthday", StringType, false))
        .add(StructField("gender", StringType, false))
        .add(StructField("country", StringType, false))

    val df = spark.read
      .format("csv")
      .schema(schema)
      .option("header", "false")
      .load(path)

    val countByName = df
      .groupBy(col("name"))
      .agg(count(col("name")).as("counter"))
      .withColumn(
        "percentage",
        round(
          col("counter") / sum(col("counter")).over(Window.partitionBy()) * 100,
          2))
      .orderBy(desc("counter"))

    println(s"Total: ${countByName.count()}")

    new java.util.Scanner(System.in).nextLine()

    spark.close()
  }

}
