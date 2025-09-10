/** SparkDF.scala
  */

package example

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.types.DataTypes
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType

object SparkGenderDF {

  val path: String = getClass.getResource("/demo.csv").getPath

  val schema =
    new StructType()
      .add(StructField("name", DataTypes.StringType, true))
      .add(StructField("paternal", DataTypes.StringType, true))
      .add(StructField("maternal", DataTypes.StringType, true))
      .add(StructField("birthday", DataTypes.StringType, true))
      .add(StructField("gender", DataTypes.StringType, true))
      .add(StructField("country", DataTypes.StringType, true))

  def main(args: Array[String]): Unit = {
    val spark =
      SparkSession.builder().appName("SparkDF").master("local[*]").getOrCreate
    spark.sparkContext.setLogLevel("ERROR")
    val df = spark.read
      .format("csv")
      .option("header", true)
      .schema(schema)
      .load(path)

    val groupByGender = df
      .select(col("gender"))
      .groupBy(col(("gender")))
      .agg(count("*").alias("counter"))
    groupByGender.show
    spark.close
  }

}
