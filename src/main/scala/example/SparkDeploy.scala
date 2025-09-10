/**
 * SparkDeploy.scala
 * @author
 *   Gerardo Jaramillo
 */

package example

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.DataTypes._

object SparkDeploy {

  def main(args: Array[String]): Unit = {
    val schema =
      new StructType()
        .add(StructField("id", IntegerType, false))
        .add(StructField("name", StringType, false))

    val spark =
      SparkSession
        .builder()
        .appName("SparkDeploy")
        .master("local[*]")
        .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    val seq: Seq[Row] = Seq(
      Row(1, "A"),
      Row(2, "B"),
      Row(3, "C"),
      Row(4, "D"),
      Row(5, "E"),
      Row(6, "F")
    )

    val rdd = spark.sparkContext.parallelize(seq)
    val df = spark.createDataFrame(rdd, schema)
    df.show()
    df.printSchema()
    spark.close()
  }

}
