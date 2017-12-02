package MedienTransparenz

import org.apache.spark.sql
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

class DataFrameMapper
{
  //session instance for spark context
  val sparkSession: SparkSession = SparkSession.builder.appName("MediaAnalysisLinearRegression").master("local[*]").getOrCreate()

  //Custom Schema for data frame to cast numeric fields
  val customSchema = StructType(
    Array
    (
      StructField("organisation", StringType, true),
      StructField("federalState", StringType, true),
      StructField("media", StringType, true),
      StructField("transferType", IntegerType, true),
      StructField("period", IntegerType, true),
      StructField("amount", FloatType, true)
    )
  )

  //create DataFrame from csv
  val dataFrame: sql.DataFrame = sparkSession.sqlContext.read
    .format("com.databricks.spark.csv")
    .option("header", "true")
    .schema(customSchema)
    .load("./data/MT-20123-20172.csv")
    .cache()



  /**
    * get DataFrame filtered by transferType
    *
    * 2   -> Zahlungen gemäß §2 MedKF-TG (Medien-Kooperationen)
    * 4   -> Zahlungen gemäß §4 MedKF-TG (Förderungen)
    * 31  -> Zahlungen gemäß §31 ORF-G (Gebühren)
    *
    * @param transferType type of transfers
    * @return  filtered DataFrame
    */
  def filterDF(transferType: String): sql.DataFrame = dataFrame.filter(dataFrame("transferType") =!= transferType)

}
