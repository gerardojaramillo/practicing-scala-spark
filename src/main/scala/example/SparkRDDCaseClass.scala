/**
 * SparkRDDCaseClass.scala
 * @author
 *   Gerardo Jaramillo
 */

package example

import example.SparkRDDTest.main
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import java.time.LocalDate
import java.time.format.DateTimeFormatter
import scala.util.Try
import scala.util.Success

import org.apache.log4j.{Level, Logger}

object SparkRDDCaseClass {

  Logger
    .getLogger("org.apache.spark")
    .setLevel(Level.ERROR)

  Logger
    .getLogger("org.eclipse.jetty.server")
    .setLevel(Level.OFF)

  val path: String = getClass
    .getResource("/demo.csv")
    .getPath

  case class Person(
      name: String,
      paternal: String,
      maternal: String,
      birthday: LocalDate,
      gender: String,
      country: String)

  val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd")

  def main(args: Array[String]): Unit = {
    val config =
      new SparkConf()
        .setAppName("SparkRDDCaseClass")
        .setMaster("local[*]")
    val sc = new SparkContext(config)
    sc.setLogLevel(Level.ERROR.toString)
    val rdd = sc.textFile(path)
    val persons = rdd.map[Option[Person]](record => {
      val cols = record.split(",").map(_.trim).toList
      cols match {
        case name :: pat :: mat :: birth :: gender :: country :: Nil =>
          Try(LocalDate.parse(birth, formatter)).toOption.map { date =>
            Person(name, pat, mat, date, gender, country)
          }
        case _ =>
          None
      }
    })

    println(s"Records number: ${persons.filter(_.isDefined).count()}")

    sc.stop()
  }

}
