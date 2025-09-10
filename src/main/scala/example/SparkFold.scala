package example

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.log4j.Level

case class Taco(name: String, price: BigDecimal)

object SparkFold {

  def main(args: Array[String]): Unit = {

    /** example #1 */
    val letter = List('A', 'B', 'C')
    println(letter.foldLeft("") { (acc, ch) => acc + ch })

    val tacoOrder: List[Taco] =
      List(
        Taco("Pastor", 10.00),
        Taco("Campechano", 11.00),
        Taco("Lengua", 9.00),
        Taco("Carnitas", 25.00)
      )

    /** standar scala collections */

    /** example #1 */
    val foldLef = tacoOrder.foldLeft(BigDecimal(0)) { (acc, taco) =>
      acc + taco.price
    }
    println(s"foldLeft: ${foldLef}")

    /** example #2 */
    val xs = List(1, 2, 3, 4)
    val res = xs.foldLeft("") { (str: String, i: Int) => str + i }
    println(s"res: ${res}")

    val sparkConf: SparkConf =
      new SparkConf().setAppName("").setMaster("local[*]")
    val sc: SparkContext = new SparkContext(sparkConf)
    sc.setLogLevel(Level.ERROR.toString)

    val rdd = sc.parallelize(tacoOrder)

    /** AGGREGATE
      */
    val seqOp = ??? // sequential operator - like fold left
    val combOp = ??? // combination operator - like regular fold
    rdd.aggregate(BigDecimal(0))(seqOp, combOp)

    // val total = rdd.map(_.price).reduce(_ + _)
    // println(s"Total: ${total}")

  }

}
