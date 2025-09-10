/**
 * SparkRDDTest.scala
 */

package example

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.log4j.Level
import org.apache.spark.rdd.RDD

case class Country(id: String, name: String)
case class City(countryid: String, name: String)

object SparkRDDTest {

  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf =
      new SparkConf()
        .setAppName("SparkRDDTest")
        .setMaster("local[*]")
    val sc: SparkContext = new SparkContext(sparkConf)
    sc.setLogLevel(Level.ERROR.toString)

    /** #1. Data */

    val numbers: List[Int] = List(1, 2, 2, 2, 1, 1, 1, 1, 3, 3, 3, 3, 3, 3, 4,
      4, 5, 5, 7, 6, 6, 9, 9, 9, 6, 6, 6, 7, 7, 8, 9)

    val numbersRdd = sc.parallelize(numbers)

    val numbersPair = numbersRdd.map(n => (n, 1))

    /**
     * groupByKey
     */
    val groupByKey: RDD[(Int, Iterable[Int])] = numbersPair.groupByKey
    // groupByKey.collect.foreach(println)

    /**
     * reduceByKey
     */
    val reduceByKey: RDD[(Int, Int)] =
      numbersPair.reduceByKey((a: Int, b: Int) => a + b)
    // reduceByKey.collect.foreach(println)

    val mapValues = numbersPair
      .mapValues(b => (b, 1))
      .reduceByKey((v1, v2) => (v1._1 + v2._1, v1._2 + v2._2))
    // mapValues.collect.foreach(println)

    val keys = numbersPair.keys.distinct.count
    println(s"count: ${keys}")

  }

}
