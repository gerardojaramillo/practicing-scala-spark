/**
 * SparkRDDs.scala
 */

package example

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object SparkRDDs {

  def main(args: Array[String]): Unit = {

    val config = new SparkConf()
      .setAppName("SparkRDDs")
      .setMaster("local[*]")
    val sc = new SparkContext(config)

    val collection: Seq[Int] = (1 to 1024).toSeq

    var rdd = sc.parallelize(collection)

    println(s"${rdd.count()}")

  }
}
