/**
 * SparkJob.scala
 */

package example

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.TimestampType

import java.util.Scanner
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row

object SparkJob {

  val path = getClass()
    .getResource("/demo.csv")
    .getPath()

  val schema =
    new StructType()
      .add("name", StringType, false)
      .add("paternal", StringType, false)
      .add("maternal", StringType, false)
      .add("birthday", TimestampType, false)
      .add("gender", StringType, false)
      .add("country", StringType, false)

  def main(args: Array[String]): Unit = {
    val config = new SparkConf()
      .setAppName("SparkJob")
      .setMaster("local[*]")
    val spark = SparkSession
      .builder()
      .config(config)
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    /** the job has not yet been created */
    val df = spark.read
      .format("csv")
      .option("header", "true")
      .schema(schema)
      .load(path)

    /** job created with show */
    df.show()

    spark.close();
  }

}
