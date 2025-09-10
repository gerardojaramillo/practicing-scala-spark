/**
 * SparkJoin.scala
 * @author
 *   Gerardo Jaramillo
 */

package example

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.TimestampType
import scala.reflect.internal.Reporter.ERROR

import org.apache.spark.sql.functions._
import java.util.Scanner
import org.apache.spark.sql.expressions.Window

object SparkJoin {

  Logger
    .getLogger("org.apache.spark")
    .setLevel(Level.ERROR)

  val peoplePath = getClass().getResource("/people.csv").getPath
  val countriesPath = getClass().getResource("/countries.csv").getPath

  val peopleSchema = new StructType()
    .add("name", StringType, false)
    .add("paternal", StringType, false)
    .add("birthday", TimestampType, false)
    .add("gender", StringType, false)
    .add("cntryCode", StringType, false)

  val cntrySchema = new StructType()
    .add("cntryCode", StringType, false)
    .add("cntryName", StringType, false)

  def main(args: Array[String]): Unit = {
    val config: SparkConf =
      new SparkConf()
        .setAppName("SparkJoin")
        .setMaster("local[*]")
    val spark: SparkSession = SparkSession
      .builder()
      .config(config)
      .getOrCreate()
    spark.sparkContext.setLogLevel(Level.ERROR.toString)

    /**
     * val countries = spark.read .format("csv") .schema(cntrySchema)
     * .option("header", "true") .load(countriesPath)
     */

    val people = spark.read
      .format("csv")
      .option("header", "true")
      .schema(peopleSchema)
      .load(peoplePath)
      .repartition(4)

    println(s"Num Partitions: ${people.rdd.getNumPartitions}")

    val groupByCountry =
      people
        .groupBy(col("cntryCode"))
        .agg(count(col("*")).as("counter"))
        .withColumn(
          "percentage",
          round(
            col("counter") * 100 / sum(col("counter")).over(
              Window.partitionBy()),
            4)
        )
        .orderBy(col("cntryCode"))
    groupByCountry.show(false)

    new java.util.Scanner(System.in).nextLine()

    spark.stop()

  }

}
