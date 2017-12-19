package MedienTransparenz

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions._

object MediaSpendingsPctPerOrg
{
  def main(args: Array[String]): Unit =
  {
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    //instance for mapper class for sparkSession
    val dataFrameMapper: DataFrameMapper = new DataFrameMapper

    val dataFrame = dataFrameMapper.filterDF(true, true, false)

    //dataFrame.show()

    //show count for transferTypes in data
    //dataFrame.groupBy("transferType").count().show()
    //dataFrame.groupBy("transferType").sum("amount").show()

    //sum amount for media for organisation over all quarters
    val mediaSumInOrgDF = dataFrame.groupBy("organisation", "federalState", "media").sum("amount").withColumnRenamed("sum(amount)", "sumMediaByOrg")
    //mediaInOrgDF.orderBy(desc("sumMediaByOrg")).show()

    //total amounts for media
    val mediaSumTotalDF = dataFrame.groupBy("media").sum("amount").withColumnRenamed("sum(amount)", "sumMediaTotal")
    //mediaTotal.orderBy(desc("sumMediaTotal")).show()

    //Table join to calculate percentage for media expenses
    val mediaJoinedDF = mediaSumInOrgDF.join(mediaSumTotalDF, "media")
    //mediaJoinedDF.show()

    //new column with calculated percentage of expenses from org for media
    val mediaPctInOrgDF =
      mediaJoinedDF
        .withColumn("%", (mediaJoinedDF("sumMediaByOrg") / mediaJoinedDF("sumMediaTotal")) * 100)
        .withColumn("sumMediaByOrg", mediaJoinedDF("sumMediaByOrg"))


    mediaPctInOrgDF
      .orderBy(desc("sumMediaTotal"), asc("organisation"), desc("%"))
      .withColumn("sumMediaTotal", format_number(mediaPctInOrgDF("sumMediaTotal"), 0))
      .withColumn("sumMediaByOrg", format_number(mediaPctInOrgDF("sumMediaByOrg"), 0))
      .withColumn("%", format_number(mediaPctInOrgDF("%"), 1))
      .drop(mediaPctInOrgDF("federalState"))
      .filter(mediaPctInOrgDF("%") >= 5)
      .filter(mediaPctInOrgDF("sumMediaTotal") >= 50000).show(100, false)
  }

}
