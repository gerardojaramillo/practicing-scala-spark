package example

import java.util.Locale
import com.github.javafaker.Faker
import java.time.LocalDate
import java.io.PrintWriter
import java.io.File
import java.util.Random
import java.time.format.DateTimeFormatter

object SparkFakerData {

  def main(args: Array[String]): Unit = {

    val rnd = new Random()
    val dateFmt = DateTimeFormatter.ofPattern("yyyy-MM-dd")

    val locales = Seq(
      new Locale("en"),
      new Locale("es"),
      new Locale("fr"),
      new Locale("de"),
      new Locale("ja"),
      new Locale("zh"),
      new Locale("ar"),
      new Locale("ru"),
      new Locale("pt")
    )
    val fakers = locales.map(new Faker(_))

    val countries = Seq(
      ("MX", "Mexico"),
      ("US", "United States"),
      ("CA", "Canada"),
      ("BR", "Brazil"),
      ("AR", "Argentina"),
      ("CL", "Chile"),
      ("FR", "France"),
      ("DE", "Germany"),
      ("IT", "Italy"),
      ("ES", "Spain"),
      ("CN", "China"),
      ("JP", "Japan"),
      ("IN", "India"),
      ("GB", "United Kingdom"),
      ("RU", "Russia"),
      ("EG", "Egypt")
    )

    val genders = Seq("Male", "Female")

    val pwPeople = new PrintWriter(new File("people.csv"))
    (1 to 100000).foreach { _ =>
      val faker = fakers(rnd.nextInt(fakers.length))
      val name = faker.name().firstName()
      val paternal = faker.name().lastName()
      val gender = genders(rnd.nextInt(genders.length))
      val birthday = LocalDate
        .of(
          1950 + rnd.nextInt(60), // 1950 - 2009
          1 + rnd.nextInt(12),
          1 + rnd.nextInt(28)
        )
        .format(dateFmt)
      val country = countries(rnd.nextInt(countries.length))._1
      pwPeople.println(s"$name,$paternal,$birthday,$gender,$country")
    }
    pwPeople.close()

    // --- Archivo países
    val pwCountries = new PrintWriter(new File("countries.csv"))
    countries.foreach { case (code, country) =>
      pwCountries.println(s"$code,$country")
    }
    pwCountries.close()

    println(
      "Archivos generados: people.txt y countries.txt con nombres internacionales.")
  }

}
