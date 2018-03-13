package MedienTransparenz

import org.apache.spark.sql
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

class DataFrameMapper
{
  //session instance for spark context

  val sparkSession: SparkSession = SparkSession.builder.appName("MediaTransparency").master("local[*]").getOrCreate()

  //Custom Schema for data frame to cast numeric fields
  val customSchema = StructType(Array(
    StructField("organisation", StringType, true),
    StructField("federalState", StringType, true),
    StructField("media", StringType, true),
    StructField("transferType", IntegerType, true),
    StructField("period", IntegerType, true),
    StructField("amount", FloatType, true)
  ))

  //create DataFrame from csv
  val dataFrame: sql.DataFrame = sparkSession.sqlContext.read
    .format("com.databricks.spark.csv")
    .option("header", "true")
    .schema(customSchema)
    .load("./dataFiles/MT-20123-20172.csv")
    .cache()



  /**
    * get DataFrame filtered by transferType
    *
    * @param mkp Zahlungen gemäß §2 MedKF-TG (Medien-Kooperationen)
    * @param fdrg Zahlungen gemäß §4 MedKF-TG (Förderungen)
    * @param gbrn Zahlungen gemäß §31 ORF-G (Gebühren)
    * @return filtered DataFrame
    */
    def filterDF(mkp: Boolean, fdrg: Boolean, gbrn: Boolean): sql.DataFrame =
    {
      if (mkp && fdrg && gbrn)
        dataFrame
      else if (mkp && fdrg && !gbrn)
        dataFrame.filter(dataFrame("transferType") =!= "31")
      else if (mkp && !fdrg && !gbrn)
        dataFrame.filter(dataFrame("transferType") =!= "31").filter(dataFrame("transferType") =!= "4")
      else if (!mkp && fdrg && !gbrn)
        dataFrame.filter(dataFrame("transferType") =!= "31").filter(dataFrame("transferType") =!= "2")
      else
        dataFrame
    }

}
