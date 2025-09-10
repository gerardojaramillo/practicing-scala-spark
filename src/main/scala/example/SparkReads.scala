/**
 * SparkReads.scala
 * @author
 *   Gerardo Jaramillo
 */

package example

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StringType
import org.apache.spark.SparkContext
import org.apache.spark.sql.Row

object SparkReads {

  def main(args: Array[String]): Unit = {

    /** read csv file. */
    val config: SparkConf = new SparkConf()
      .setAppName("SparkReads")
      .setMaster("local[*]")
    val spark: SparkSession =
      SparkSession
        .builder()
        .config(config)
        .getOrCreate()
    import org.apache.log4j.Level.ERROR
    spark.sparkContext.setLogLevel(ERROR.toString)

    val schema = new StructType()
      .add("field", StringType, false)

    val rdd = spark.sparkContext.textFile("source.txt")
    val rowRDD = rdd.map(_.split(",")).map { arr =>
      Row(arr(1), arr(2))
    }

    val textDF = spark.read
      .format("text")
      .option("delimiter", ",")
      .load()

    /** csv format */
    val csvDF = spark.read
      .format("csv")
      .option("header", "false")
      .schema(schema)
      .load("file.csv")

    /** json format */
    val jsonDF = spark.read
      .format("json")
      .option("multiLine", "true")
      .schema(schema)
      .load("file.json")

    /** parquet format */
    val parquetDF = spark.read
      .schema(schema)
      .parquet("file.parquet")

    /** socket format */
    val socketDF = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", "1234")
      .load()
      .selectExpr("CAST(value) AS STRING")

  }

}
