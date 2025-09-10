/**
 * SparkSchema.scala
 * @author
 *   Gerardo Jaramillo
 */

package example

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StringType

object SparkSchema {

  val path = getClass().getResource("/source.csv").getPath

  def main(args: Array[String]): Unit = {
    val schema = new StructType()
      .add("name", StringType, false)
      .add("paternal", StringType, false)
    val config: SparkConf =
      new SparkConf()
        .setAppName("SparkSchema")
        .setMaster("local[*]")
    val spark: SparkSession =
      SparkSession
        .builder()
        .config(config)
        .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    val df = spark.read
      .format("csv")
      .schema(schema)
      .option("header", "true")
      .load(path)
    import org.apache.spark.sql.functions._
    df.filter(isnotnull(col("paternal"))).show()
    spark.close()

  }

}
