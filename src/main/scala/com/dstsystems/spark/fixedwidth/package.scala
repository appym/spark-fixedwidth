package com.dstsystems.spark

import com.databricks.spark.csv.util.TextFile
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SQLContext}
/**
  * Created by dt205202 on 8/14/17.
  */
package object fixedwidth {
  implicit class FixedWidthContext(sqlContext: SQLContext) extends Serializable {
    def fixedFile(
                   filePath: String,
                   fixedWidths: Array[Int],
                   schema: StructType = null,
                   useHeader: Boolean = true,
                   mode: String = "FAILFAST",
                   comment: Character = null,
                   ignoreLeadingWhiteSpace: Boolean = true,
                   ignoreTrailingWhiteSpace: Boolean = true,
                   charset: String = TextFile.DEFAULT_CHARSET.name(),
                   inferSchema: Boolean = false): DataFrame = {
      val fixedWidthRelation = new FixedWidthRelation(
        () => TextFile.withCharset(sqlContext.sparkContext, filePath, charset),
        location = Some(filePath),
        useHeader = useHeader,
        comment = comment,
        parseMode = mode,
        fixedWidths = fixedWidths,
        ignoreLeadingWhiteSpace = ignoreLeadingWhiteSpace,
        ignoreTrailingWhiteSpace = ignoreTrailingWhiteSpace,
        userSchema = schema,
        inferSchema = inferSchema,
        treatEmptyValuesAsNulls = false)(sqlContext)
      sqlContext.baseRelationToDataFrame(fixedWidthRelation)
    }
  }
}
