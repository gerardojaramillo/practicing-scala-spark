/**
 * Engine.scala
 */

package example

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.TimestampType
import org.apache.spark.sql.Column
import org.apache.spark.sql.catalog
import org.apache.spark.sql.functions._
import org.apache.log4j.{Level, Logger}
import example.SparkRead.file

object Engine {

  Logger
    .getLogger("org.apache.spark")
    .setLevel(Level.ERROR)

  val schema = StructType(
    Array(
      new StructField("n", IntegerType, true),
      new StructField("id", IntegerType, true),
      new StructField("case number", StringType, true),
      new StructField("date", TimestampType, true)
    )
  )

  val path: String =
    getClass
      .getResource("/Chicago_Crimes_2001_to_2004.csv")
      .getPath

  def main(args: Array[String]): Unit = {
    val t = System.currentTimeMillis()
    val spark = SparkSession
      .builder()
      .appName("Engine")
      .master("local[*]")
      .config("spark.executor.memory", "4g")
      .config("spark.driver.memory", "2g")
      .config("spark.sql.shuffle.partitions", "20")
      .getOrCreate()
    spark.sparkContext.setLogLevel(Level.ERROR.toString)
    val df = spark.read
      .format("csv")
      .option("header", true)
      .option("delimiter", ",")
      .option("timestampFormat", "MM/dd/yyyy hh:mm:ss a")
      .schema(schema)
      .load(path)

    val fromTimestamp = java.sql.Timestamp.valueOf("2001-01-01 00:00:00")
    val toTimestamp = java.sql.Timestamp.valueOf("2001-01-01 23:59:59")

    val filtered = df
      .filter(
        col("date")
          .between(fromTimestamp, toTimestamp)
      )

    println(s"filtered: ${filtered.count()}")

    spark.close()
    println(s"${(System.currentTimeMillis() - t).toDouble / 1000} secs.")
  }

}
