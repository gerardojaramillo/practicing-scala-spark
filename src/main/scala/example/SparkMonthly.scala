/** SparkMonthly.scala
  */

package example

import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DataTypes
import org.apache.spark.sql.types.DecimalType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType

import java.time.LocalDate
import java.time.format.DateTimeFormatter

object SparkMonthly {

  val spark =
    SparkSession.builder.appName("SparkMonthly").master("local[*]").getOrCreate
  spark.sparkContext.setLogLevel("ERROR")

  val path: String = getClass.getResource("/sells.csv").getPath

  val schema =
    new StructType()
      .add(StructField("productid", DataTypes.LongType, false))
      .add(StructField("cost", DecimalType(10, 2), false))
      .add(StructField("storeid", DataTypes.LongType, false))
      .add(StructField("createdat", DataTypes.TimestampType, false))

  def generate(month: Int, dayOfMonth: Int): Seq[String] = {
    val startDate = LocalDate.of(2024, month, dayOfMonth)
    val endDate = startDate.withDayOfMonth(startDate.lengthOfMonth)
    val dates =
      startDate.toEpochDay.to(endDate.toEpochDay).map(LocalDate.ofEpochDay)
    dates.map(_.format(DateTimeFormatter.ISO_LOCAL_DATE))
  }

  def read(path: String): Dataset[Row] =
    spark.read
      .format("csv")
      .schema(schema)
      .option("header", "true")
      .load(path)

  val datesDF =
    spark
      .createDataFrame(
        generate(1, 1)
          .map(Tuple1(_))
      )
      .toDF("joinDate")

  def main(args: Array[String]): Unit = {
    val dates = datesDF

    val createdat =
      read(path).withColumn(
        "joinDate",
        date_format(col("createdat"), "yyyy-MM-dd")
      )
    // createdat.show

    val joined = dates.join(createdat, Seq("joinDate"), "left")

    joined
      .groupBy(col("joinDate"))
      .agg(
        coalesce(sum(col("cost")), lit(0))
          .alias("total_cost"),
        count(col("productid")).alias("case_count")
      )
      .orderBy(col("joinDate"))

    joined.show(50, false)
  }

}
