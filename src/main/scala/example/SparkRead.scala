/** SparkRead.scala
  */

package example

import org.apache.spark.SparkConf

import org.apache.spark.sql.Row
import org.apache.spark.sql.Dataset
import org.apache.spark.SparkContext
import scala.io.Source
import scala.io.Codec
import scala.util.Using
import scala.util.Try
import scala.util.Success
import scala.util.Failure

object SparkRead {

  val file: String =
    getClass().getResource("/Chicago_Crimes_2001_to_2004.csv").getPath

  def source(): Try[Seq[String]] = {
    Using(Source.fromFile(file)(Codec.UTF8)) { br =>
      br.getLines()
        .filter(!_.isEmpty())
        .zipWithIndex
        .map { case (line, lineno) =>
          line.trim
        }
        .toSeq
    }
  }

  /** Crime case class.
    *
    * @param n
    * @param id
    * @param cn
    */
  case class Crime(n: String, id: String, cn: String)

  type regExpr = String

  val splitLine: regExpr = ",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)"

  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf =
      new SparkConf()
        .setAppName("SparkRead")
        .setMaster("local[*]")
    val sc: SparkContext = new SparkContext(sparkConf)

    val rdd = source() match {
      case Success(raw)       => sc.parallelize(raw)
      case Failure(exception) => ???
    }

    val rddParsed = rdd.flatMap(line => {
      val split = line.split(splitLine, -1).map(_.trim)
      split.length match {
        case 23 => {
          val (n, id, caseNo) = (split(0), split(1), split(2))
          Some(Crime(n, id, caseNo))
        }
        case _: Int => None
      }
    })

    val grouped = rddParsed.groupBy(_.id)

    val total = grouped.count()

    println(s"Total: ${total}")
    grouped.take(10).foreach(println)

  }

}
