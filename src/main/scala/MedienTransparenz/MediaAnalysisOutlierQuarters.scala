package MedienTransparenz

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions.round

object MediaAnalysisOutlierQuarters
{
  def main(args: Array[String]): Unit =
  {
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    //instance for mapper class for sparkSession
    val dataFrameMapper: DataFrameMapper = new DataFrameMapper

    val dataFrame = dataFrameMapper.dataFrame

    val quarterData = dataFrame.select("period", "amount").groupBy("period").sum("amount").orderBy("period")

    quarterData.show()

    quarterData.describe().show()
  }

}
