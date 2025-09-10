package example

import org.apache.spark.sql.SparkSession
import org.apache.log4j.Level
import org.apache.spark.sql.Row
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import scala.annotation.meta.param
import org.apache.spark.rdd.RDD

object SparkRDD {

  def main(args: Array[String]): Unit = {

    val sparkConf: SparkConf =
      new SparkConf()
        .setAppName("SparkRDD")
        .setMaster("local[*]")
    val sc: SparkContext = new SparkContext(sparkConf)
    sc.setLogLevel(Level.ERROR.toString)

    val numberRdd = sc.parallelize(
      List(("Mx", "Aguacalientes"), ("EU", "United States"), ("CN", "Canada"))
    )
    numberRdd.collect().foreach(println)

    /** Pair RDDs (Key-Value Pairs) */
    val countries =
      List(("Mx", "Mexico"), ("Eu", "Estados Unidos"), ("CA", "Canada"))

    val rdd = sc.parallelize(countries)

    case class Wikipedia(country: String, title: String)
    val topics = List(
      Wikipedia("Mx", "La Noticia"),
      Wikipedia("Eu", "La Noticia"),
      Wikipedia("Ca", "La Noticia")
    )

    val rddWikipedia = sc.parallelize(topics)
    val pairRDD =
      rddWikipedia.map(wikipedia => (wikipedia.country, wikipedia))

    val titles: RDD[(String, String)] = pairRDD.mapValues(_.title)
    titles.foreach(println)

    /** PairRDD adds a number of extra useful additional methods */
    // pairRDD.groupByKey
    // pairRDD.reduceByKey
    // pairRDD.join

    /** Transformations implement the next methods */
    /** #1 groupByKey */
    /** #2 reduceByKey */
    /** #3 mapValues */
    /** #4 keys */
    /** #5 join */
    /** #6 leftOuterJoin/rightOuterJoin */

    case class Country(code: String, name: String, population: Int)
    val countries2: List[Country] = List(
      Country("Mx", "México", 130861007),
      Country("Eu", "United States", 345426571),
      Country("Ca", "Canada", 39742430)
    )

    val countriesRdd = sc.parallelize(countries2)
    val countriesPairRdd = countriesRdd.map(country => (country.code, country))

    println("mapValues:")
    val mv = countriesPairRdd.mapValues(_.population)
    mv.collect.foreach(println)

    /** Regular Scala Collections */
    val cities = List(
      ("Mx", "Ciudad de México"),
      ("Mx", "Monterrey"),
      ("Mx", "Guadalajara"),
      ("Eu", "Chicago"),
      ("Eu", "New York"),
      ("Eu", "Manhatan")
    )

    val result = cities.groupBy(_._1)
    result.foreach(println)
    println

    val ages = List(2, 52, 44, 23, 17, 14, 12, 82, 51, 64)
    val grouped = ages.groupBy { age =>
      if (age >= 18 && age > 65) "adult"
      else if (age < 18) "child"
      else "senior"
    }
    grouped.foreach(println)
    println

    /** #? example */
    case class Event(organizer: String, budget: Int)
    val events: List[Event] = List(
      Event("Some Even #1", 3),
      Event("Some Even #2", 5),
      Event("Some Even #3", 7),
      Event("Some Even #1", 9),
      Event("Some Even #2", 11)
    )

    val eventsRdd = sc.parallelize(events).map { e => (e.organizer, e.budget) }
    val groupedRdd = eventsRdd.groupByKey
    groupedRdd.collect.foreach(println)
    println

    /** #? example more efficient - only operated on the values */
    val reducedByKey: RDD[(String, Int)] =
      eventsRdd.reduceByKey((a: Int, b: Int) => a + b)
    reducedByKey.collect.foreach(println)
    println

    /** #? mapValues - transformation */
    println("mapValue:")
    val mapValues = eventsRdd.mapValues { r => r + 1 }
    mapValues.collect.foreach(println)
    println

    /** #? countByKey - action */

  }

}
