package com.dstsystems.spark.fixedwidth

import org.apache.spark.{SparkContext, SparkException}
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.sql.types._
import org.specs2.mutable.Specification
import org.specs2.specification.After

/**
  * Created by dt205202 on 8/14/17.
  */
trait FixedWidthSetup extends After {
  protected def fruit_resource(name: String = ""): String =
    s"src/test/resources/fruit_${name}_fixedwidth.txt"

  protected val fruitWidths = Array(3, 10, 5, 4)
  protected val fruitSize = 7
  protected val malformedFruitSize = 5
  protected val fruitFirstRow = Seq(56, "apple", true, 0.56)

  protected val fruitSchema = StructType(Seq(
    StructField("val", IntegerType),
    StructField("name", StringType),
    StructField("avail", BooleanType),
    StructField("cost", DoubleType)
  ))

  val sqlContext: SQLContext = new SQLContext(new SparkContext("local[2]", "FixedWidthSuite"))

  def after = sqlContext.sparkContext.stop()
}

class FixedWidthSpec extends Specification with FixedWidthSetup {

  protected def sanityChecks(resultSet: DataFrame) = {
    resultSet.show()
    resultSet.collect().length mustEqual fruitSize

    val head = resultSet.head
    head.length mustEqual fruitWidths.length
    head.toSeq.toList shouldEqual fruitFirstRow
}

  "FixedwidthParser" should {
    "Parse a basic fixed width file, successfully" in {
      val result = sqlContext.fixedFile(fruit_resource(), fruitWidths, fruitSchema,
        useHeader = false)
      sanityChecks(result)
    }

    "Parse a fw file with headers, and ignore them" in {
      val result = sqlContext.fixedFile(fruit_resource("w_headers"), fruitWidths,
        fruitSchema, useHeader = true)
      sanityChecks(result)
    }

    "Parse a fw file with overflowing lines, and ignore the overflow" in {
      val result = sqlContext.fixedFile(fruit_resource("overflow"), fruitWidths,
        fruitSchema, useHeader = false)
      sanityChecks(result)
    }

    "Parse a fw file with underflowing lines, successfully " in {
      val result = sqlContext.fixedFile(fruit_resource("underflow"), fruitWidths,
        fruitSchema, useHeader = false)
      sanityChecks(result)
    }

    "Parse a basic fw file without schema and without inferring types, successfully" in {
      val result = sqlContext.fixedFile(fruit_resource(), fruitWidths,
        useHeader = false, inferSchema = false)
      sanityChecks(result)
    }

    "Parse a basic fw file without schema, and infer the schema" in {
      val result = sqlContext.fixedFile(fruit_resource(), fruitWidths,
        useHeader = false, inferSchema = true)
      sanityChecks(result)
    }

    "Parse a malformed fw and schemaless file in PERMISSIVE mode, successfully" in {
      val result = sqlContext.fixedFile(fruit_resource("malformed"), fruitWidths,
        useHeader = false, mode = "PERMISSIVE")
      result.show()
      result.collect().length mustEqual fruitSize
    }

    "FAIL to parse a malformed fw file with schema in FAILFAST mode" in {
      def fail = {
        sqlContext.fixedFile(fruit_resource("malformed"), fruitWidths,
          fruitSchema, useHeader = false, mode = "FAILFAST").collect()
      }
      fail must throwA[SparkException]
    }

    "FAIL to parse a fw file with the wrong format" in {
      def fail = {
        sqlContext.fixedFile(fruit_resource("wrong_schema"), fruitWidths,
          fruitSchema, useHeader = false).collect()
      }
      fail must throwA[SparkException]
    }
  }

}
