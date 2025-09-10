/**
 * SparkParquet.scala
 */

package example

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.TimestampType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.IntegerType

object SparkParquet extends App {

  val t = System.currentTimeMillis()

  val schema = new StructType()
    .add("Date", TimestampType, false)
    .add("Primary Type", StringType, false)
    .add("Community Area", StringType, false)
    .add("Year", IntegerType, false)
    .add("Month", IntegerType, false)

  val config = new SparkConf()
    .setAppName("SparkParquet")
    .setMaster("local[*]")

  val spark = SparkSession
    .builder()
    .config(config)
    .getOrCreate()
  spark.sparkContext.setLogLevel("ERROR")

  val parquetPath = "/Users/millodev/Workspace/demo-chicagocrime-app/data/"

  val df = spark.read
    .schema(schema)
    .parquet(parquetPath)

  println(s"repartitions: ${df.rdd.getNumPartitions}")

  println(s"Count All: ${df.count()}")

  // new java.util.Scanner(System.in).nextLine()

  spark.close()

  println(s"${System.currentTimeMillis() - t}".toDouble / 1000)

}
