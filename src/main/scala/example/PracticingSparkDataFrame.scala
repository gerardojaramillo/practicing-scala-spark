/**
 * PracticingSparkDataFrame.scala
 */

package example

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.IntegerType

object PracticingSparkDataFrame {

  val schema = new StructType()
    .add("id", LongType, false)
    .add("name", StringType, false)
    .add("age", IntegerType, false)

  def main(args: Array[String]): Unit = {
    val config = new SparkConf()
      .setAppName("PracticingSparkDataFrame")
      .setMaster("local[*]")
    val spark = SparkSession.builder().config(config).getOrCreate()
    val seq =
      Seq(Row(1L, "Name A", 28), Row(2L, "Name B", 24), Row(3L, "Name C", 40))

    val users = spark.sparkContext.parallelize(seq)
    val df = spark.createDataFrame(users, schema)

    println(df.first())
    df.printSchema()
    df.show()
    spark.close()
  }

}
