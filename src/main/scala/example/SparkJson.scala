/**
 * SparkJson.scala
 * @author
 *   Gerardo Jaramillo
 */

package example

import org.apache.spark.SparkConf
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object SparkJson {

  Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
  Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)
  val path = getClass.getResource("/people.json").getPath

  def main(args: Array[String]): Unit = {
    val config = new SparkConf()
      .setAppName("SparkJson")
      .setMaster("local[*]")
    val spark = SparkSession
      .builder()
      .config(config)
      .getOrCreate()
    spark.sparkContext.setLogLevel(Level.ERROR.toString)
    val df = spark.read
      .format("json")
      .option("multiLine", "true")
      .load(path)
    df.show()
    df.printSchema()
    spark.stop()
    spark.close()
  }

}
