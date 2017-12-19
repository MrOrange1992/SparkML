package FlightAnalysis

import org.apache.spark.sql
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

class FlightDataMapper
{
  val sparkSession: SparkSession = SparkSession.builder.appName("FlightAnalysis").master("local[*]").getOrCreate()


  //Custom Schema for data frame to cast numeric fields
  val customSchema = StructType(
    Array
    (
      StructField("YEAR", IntegerType, true),
      StructField("QUARTER", IntegerType, true),
      StructField("MONTH", IntegerType, true),
      StructField("DAY_OF_MONTH", IntegerType, true),
      StructField("DAY_OF_WEEK", IntegerType, true),
      StructField("UNIQUE_CARRIER", StringType, true),
      StructField("AIRLINE_ID", IntegerType, true),
      StructField("ORIGIN_AIRPORT_ID", IntegerType, true),
      StructField("ORIGIN_CITY_NAME", StringType, true),
      StructField("ORIGIN_STATE_ABR", StringType, true),
      StructField("DEST_AIRPORT_ID", IntegerType, true),
      StructField("DEST_CITY_NAME", StringType, true),
      StructField("DEST_STATE_ABR", StringType, true),
      StructField("DEP_TIME", IntegerType, true),
      StructField("DEP_DELAY", FloatType, true),
      StructField("ARR_TIME", IntegerType, true),
      StructField("ARR_DELAY", FloatType, true),
      StructField("CANCELLED", FloatType, true),
      StructField("AIR_TIME", FloatType, true),
      StructField("DISTANCE", FloatType, true)
    )
  )

  //create DataFrame from csv
  val rawData: sql.DataFrame = sparkSession.sqlContext.read
    .format("com.databricks.spark.csv")
    .schema(customSchema)
    //.option("nullValue", "null")
    //.option("treatEmptyValuesAsNulls", "true")
    .option("header", "true")
    .option("delimiter", ",")
    .option("escape", "\"")
    .load("./data/FlightData.csv")
    .cache()

  val dataFrame: sql.DataFrame = rawData.filter(rawData("CANCELLED") === 0f)

}
