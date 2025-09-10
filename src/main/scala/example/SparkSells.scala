/** SparkSells.scala
  */

package example

import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DataTypes._
import org.apache.spark.sql.types.DecimalType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType

import scala.util.Try
import scala.util.Success
import scala.util.Failure

object SparkSells {

  val spark =
    SparkSession
      .builder()
      .appName("SparkSells")
      .master("local[*]")
      .getOrCreate
  spark.sparkContext.setLogLevel("ERROR")

  val path: String = getClass.getResource("/sells.csv").getPath

  val schema =
    new StructType()
      .add(StructField("productid", LongType, false))
      .add(StructField("cost", DecimalType(10, 2), false))
      .add(StructField("storeid", LongType, false))
      .add(StructField("createdat", TimestampType, false))

  def read(): Dataset[Row] = {
    spark.read
      .format("csv")
      .schema(schema)
      .option("header", "true")
      .load(path)
  }

  def main(args: Array[String]): Unit = {
    // filter("2024-07-08 00:00:00", "2024-07-08 23:59:59").show(false)
    filter(Some(4001)) fold (_.printStackTrace, _.show(false))
  }

  def groupBy: Dataset[Row] = {
    read()
      .select(col("productid"), col("cost"))
      .groupBy(col("productid"))
      .agg(
        count(col("productid")).alias("items"),
        sum(col("cost")).alias("total")
      )
      .sort(desc("productid"))
  }

  def filter(startDate: String, endDate: String) =
    read().filter(col("createdat").between(startDate, endDate))

  def filter(productid: Option[Long]): Try[Dataset[Row]] = {
    productid match {
      case None =>
        Failure(new IllegalArgumentException(s"Illegal argument ${productid}"))
      case Some(id) =>
        Success {
          read filter (col("productid") === id)
        }
    }
  }

}
