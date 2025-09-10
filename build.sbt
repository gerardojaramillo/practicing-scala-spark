import Dependencies._

ThisBuild / scalaVersion := "2.13.12"

/** ThisBuild / scalaVersion := "2.12.13" */
/** ThisBuild / scalaVersion := "2.12.17" */
ThisBuild / version := "0.1.0-SNAPSHOT"
ThisBuild / organization := "com.example"
ThisBuild / organizationName := "example"
scalacOptions ++= Seq("-language:implicitConversions", "-deprecation")

lazy val root = (project in file("."))
  .settings(
    assembly / assemblyJarName := "practicing-scala-spark.jar",
    /** assembly / mainClass := Some("example.SparkDeploy"), */
    name := "practicing-scala-spark",
    libraryDependencies += munit % Test,
    libraryDependencies += "org.apache.spark" %% "spark-core" % "3.5.1",
    libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.5.1",
    libraryDependencies += "org.apache.spark" %% "spark-streaming" % "3.5.1",
    libraryDependencies += "org.apache.spark" %% "spark-sql-kafka-0-10" % "3.5.1",

    libraryDependencies += "com.github.javafaker" % "javafaker" % "1.0.2"

    /**    libraryDependencies += "org.apache.spark" %% "spark-core" % "3.2.4" % "provided",
      *    libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.2.4" % "provided",
      *    libraryDependencies += "org.apache.spark" %% "spark-streaming" % "3.2.4" % "provided",
      *    libraryDependencies += "org.apache.spark" %% "spark-sql-kafka-0-10" % "3.2.4"
      */

  )
javaOptions in console ++= Seq(
  "--add-exports=java.base/sun.nio.ch=ALL-UNNAMED",
  "--add-opens=java.base/java.lang.reflect=ALL-UNNAMED",
  "--add-opens=java.base/java.nio=ALL-UNNAMED",
  "--add-opens=java.base/java.util=ALL-UNNAMED",
  "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED"
)
javaOptions in run ++= Seq(
  "--add-exports=java.base/sun.nio.ch=ALL-UNNAMED",
  "--add-exports=java.base/java.lang=ALL-UNNAMED"
)

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case PathList("META-INF", "services", xs @ _*) =>
    MergeStrategy.filterDistinctLines
  case "reference.conf" => MergeStrategy.concat
  case _                => MergeStrategy.first
}

/** mainClass in assembly := Some("example.SparkDeploy") */
