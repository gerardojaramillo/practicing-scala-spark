/**
 * SparkExplain.scala
 * @author
 *   Gerardo Jaramillo
 */

package example

import org.apache.spark.sql.SparkSession
import org.apache.log4j.{Level, Logger}

object SparkExplain {

  Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
  
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("SparkExplain")
      .master("local[*]")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    import spark.implicits._
    val df = Seq(
      ("MX", 1),
      ("MX", 2),
      ("US", 3),
      ("US", 4),
      ("CA", 5),
      ("BR", 6)
    ).toDF("code", "name")
    println("👉 Plan físico sin groupBy:")
    df.explain(true)

    import org.apache.spark.sql.functions._
    val grouped = df.groupBy(col("code")).count()
    println("👉 Ahora con groupBy (dispara shuffle):")
    grouped.explain(true)
    grouped.show()

    spark.close()

  }

}
