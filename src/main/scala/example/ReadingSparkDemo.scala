/**
 * ReadingSparkDemo.scala
 * @author
 *   Gerardo Jaramillo
 */

package example

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.DataTypes
import org.apache.spark.sql.types.Metadata

import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

object ReadingSparkDemo {

  val schema =
    new StructType()
      .add(StructField("name", DataTypes.StringType, false))
      .add(StructField("paternal", DataTypes.StringType, false))
      .add(StructField("maternal", DataTypes.StringType, false))
      .add(StructField("birthday", DataTypes.DateType, false))
      .add(StructField("gender", DataTypes.StringType, false))
      .add(StructField("country", DataTypes.StringType, false))

  def main(args: Array[String]): Unit = {
    val path = getClass().getResource("/demo.csv").getPath
    val spark =
      SparkSession
        .builder()
        .appName("Progama")
        .master("local[*]")
        .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    val df = spark.read
      .format("csv")
      .schema(schema)
      .option("header", true)
      .load(path)

    val countByCountry =
      df.groupBy(col("country"))
        .agg(count(col("*")).as("counter"))
        .withColumn(
          "percentage",
          round(
            col("counter") / sum(col("counter")).over(
              Window.partitionBy()) * 100,
            2))
        .orderBy(col("country"))
    countByCountry.show()

    /**
     * val countByGender = df.groupBy(col("gender"))
     * .agg(count(col("gender")).as("counter")) .withColumn( "percentage",
     * round( col("counter") / sum("counter").over(Window.partitionBy()) * 100,
     * 2)) .orderBy(col("gender")) countByGender.show()
     */
    spark.close
  }

}
