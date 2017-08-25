name := "spark-fixedwidth"

version := "1.0"

scalaVersion := "2.10.6"

// https://mvnrepository.com/artifact/com.databricks/spark-csv_2.10
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-sql" % "1.6.0" % "provided",
  "org.apache.spark" %% "spark-core" % "1.6.0" % "provided",
  "org.specs2" %% "specs2-core" % "3.7" % "test",
  "org.scalatest" %% "scalatest" % "2.2.1" % "test")
