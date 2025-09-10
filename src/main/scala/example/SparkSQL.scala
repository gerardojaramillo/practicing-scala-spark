/**
 * SparkSQL.scala
 * @author
 *   Gerardo Jaramillo
 */

package example

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.TimestampType

import org.apache.log4j.{Level, Logger}

object SparkSQL {

  Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)

  val path = getClass().getResource("/demo.csv").getPath()

  def main(args: Array[String]): Unit = {

    val schema = new StructType()
      .add("name", StringType, false)
      .add("paternal", StringType, false)
      .add("maternal", StringType, false)
      .add("birthday", TimestampType, false)
      .add("gender", StringType, false)
      .add("country", StringType, false)

    val config: SparkConf =
      new SparkConf()
        .setAppName("SparkSQL")
        .setMaster("local[*]")
    val spark: SparkSession =
      SparkSession
        .builder()
        .config(config)
        .getOrCreate()

    spark.sparkContext.setLogLevel(Level.ERROR.toString)

    val df = spark.read
      .format("csv")
      .option("header", "true")
      .schema(schema)
      .load(path)
    df.createOrReplaceTempView("all")

    val gender =
      spark
        .sql("""
            | SELECT DISTINCT(gender) 
            | FROM all 
            | ORDER BY gender""".stripMargin)
        .show()

    spark
      .sql("""
        | SELECT COUNT(*) counter
        | FROM all
    """.stripMargin)
      .show()

    spark
      .sql("""
        | SELECT gender, COUNT(gender) counter, ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) AS percentage
        | FROM all 
        | GROUP BY gender 
        | ORDER BY GENDER ASC
      """.stripMargin)
      .show()

    spark.close()

  }

}
