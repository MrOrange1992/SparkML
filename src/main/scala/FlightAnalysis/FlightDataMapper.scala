package FlightAnalysis

import org.apache.spark.sql
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

class FlightDataMapper
{
  val sparkSession: SparkSession = SparkSession.builder.appName("FlightAnalysis").master("local[*]").getOrCreate()


  //create DataFrame from csv
  val dataFrame: sql.DataFrame = sparkSession.sqlContext.read
    .format("com.databricks.spark.csv")
    .option("header", "true")
    .load("./data/FlightData_Snippet.csv")
    .cache()

}
