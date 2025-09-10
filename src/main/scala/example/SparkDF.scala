/**
 * SparkDF.scala
 * @author
 *   Gerardo Jaramillo
 */

package example

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StringType

object SparkDF {

  Logger
    .getLogger("org.apache.spark")
    .setLevel(Level.ERROR)

  def main(args: Array[String]): Unit = {

    val config: SparkConf =
      new SparkConf()
        .setAppName("SparkDF")
        .setMaster("local[*]")
    val spark: SparkSession =
      SparkSession
        .builder()
        .config(config)
        .getOrCreate()
    spark.sparkContext.setLogLevel(Level.ERROR.toString)

    import spark.implicits._
    val personsDF = Seq(("Michael", "USA", "Male", 40)).toDF(
      "Name",
      "Country",
      "Gender",
      "Age")

    personsDF.show()

    val schema = new StructType()
      .add("Code", StringType, false)
      .add("Country", StringType, false)
    val cities = Seq(
      Row("Mx", "México"),
      Row("Mx", "México"),
      Row("Mx", "México")
    ).toList
    val rdd = spark.sparkContext.parallelize(cities)
    val df = spark.createDataFrame(rdd, schema)
    df.show()

    spark.close()
  }
}
