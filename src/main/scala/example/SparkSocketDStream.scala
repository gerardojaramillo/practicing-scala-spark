/** SparkSocketDStream.scala
  */

package example

import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.StreamingContext

import org.apache.spark.streaming.Seconds
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.DStream

object SparkSocketDStream {

  val spark = SparkSession.builder
    .appName("SparkSocketDStream")
    .master("local[*]")
    .getOrCreate
  val ssc = new StreamingContext(spark.sparkContext, Seconds(3))

  def readFromSocket() {
    val stream: DStream[String] =
      ssc.socketTextStream(
        "127.0.0.1",
        1234,
        StorageLevel.MEMORY_AND_DISK_SER_2
      )

    val words: DStream[String] = stream.flatMap(line => line.split(" "))
    words.print

    ssc.start
    ssc.awaitTermination
  }

  def readFile(path: String) {
    val stream: DStream[String] = ssc.textFileStream(path)
    stream.foreachRDD(rdd => {
      if (!rdd.isEmpty()) {
        val words = rdd.flatMap(_.split("\\s+")).map(_.trim)
        words.collect().foreach(println)
      }
    })

    ssc.start
    ssc.awaitTermination
  }

  def main(args: Array[String]): Unit = {
    val path: String =
      "/Users/millodev/Workspace/practicing-scala-spark/src/main/resources/source.csv"
    readFile(path)
  }

}
