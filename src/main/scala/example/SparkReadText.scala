/**
 * SparkReadText.scala
 * @author
 *   Gerardo Jaramillo
 */

package example

import org.apache.log4j.Level.ERROR
import org.apache.parquet.format.IntType
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType

object SparkReadText {

  val path: String = getClass()
    .getResource("/source.txt")
    .getPath()

  def main(args: Array[String]): Unit = {
    val config: SparkConf = new SparkConf()
      .setAppName("SparkReadText")
      .setMaster("local[*]")
    val spark: SparkSession =
      SparkSession
        .builder()
        .config(config)
        .getOrCreate()
    spark.sparkContext.setLogLevel(ERROR.toString)
    val rdd: RDD[String] = spark.sparkContext.textFile(path)

    val header = rdd.first()
    val filteredRDD = rdd.filter(_ != header)

    /** option #01 Row solo da valores posicionales */
    val rowRDD: RDD[Row] = filteredRDD.map(_.split(",")).map { arr =>
      Row(arr(0), arr(1), arr(2), arr(3))
    }

    /**
     * option #02 remember we need spark.implicits._ to transform to DataFrame y
     * Spark infiere el tipo de datos
     */
    import spark.implicits._
    val rowDF = filteredRDD
      .map(_.split(",").map(_.trim))
      .map { arr =>
        (arr(0), arr(1), arr(2), arr(3).toInt)
      }
      .toDF("name", "country", "gender", "age")

    /**
     * option #03 utilizando un schema para implicitamente decirte el tipo de
     * dato por field y usando la función spark.createDataFrame.
     */
    val schema = new StructType()
      .add("name", StringType, false)
      .add("country", StringType, false)
      .add("gender", StringType, false)
      .add("age", IntegerType, false)
    val df = spark.createDataFrame(rowRDD, schema)

    df.printSchema()
    df.show()

    spark.close()
  }

}
