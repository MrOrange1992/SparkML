package MedienTransparenz

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions.round

object MediaAnalysisOutlierQuarters
{
  def main(args: Array[String]): Unit = {
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Spark Context, using all Cores of local machine
    val spark = SparkSession.builder.appName("MediaAnalysisOutlierQuarters").master("local[*]").getOrCreate()


    //Custom Schema for data frame to cast numeric fields
    val customSchema = StructType(Array(
      StructField("RECHTSTRÄGER", StringType, true),
      StructField("QUARTAL", IntegerType, true),
      StructField("BEKANNTGABE", IntegerType, true),
      StructField("LEERMELDUNG", IntegerType, true),
      StructField("MEDIUM_MEDIENINHABER", StringType, true),
      StructField("EURO", DataTypes.createDecimalType(38,10), true)
    ))

    /*
     csv sanitations ~lukas:
      deleted all unused columns
      replaced all 'null' with ''
      replaced all '""' with ''
   */
    val data = spark.sqlContext.read
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .schema(customSchema)
      .load("./data/MedienTransparenz-20123-20171.csv")
      .cache()
    //data.printSchema()
    //data.show()

    val quarterData = data.select(data("QUARTAL"), data("EURO")).groupBy(data("QUARTAL")).sum("EURO").as("EURO").orderBy("QUARTAL")

    val roundedData = quarterData.withColumn("sum(EURO)", round(quarterData("sum(EURO)"), 2))

    roundedData.show()

  }

}
