package FlightAnalysis

import org.apache.spark.sql
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.types._

class FlightDataMapper
{
  val sparkSession: SparkSession = SparkSession.builder.appName("FlightAnalysis").master("local[*]").getOrCreate()


  ///"YEAR","QUARTER","MONTH","DAY_OF_MONTH","DAY_OF_WEEK","AIRLINE_ID","ORIGIN_AIRPORT_ID","DEST_AIRPORT_ID","CRS_DEP_TIME","DEP_DELAY","DEP_DELAY_NEW","CRS_ARR_TIME","CANCELLED","DISTANCE","DISTANCE_GROUP",
  //2017,1,1,18,3,19930,14747,14831,"0550",-5.00,0.00,"0753",0.00,696.00,3,

  //Custom Schema for data frame to cast numeric fields
  val customSchema = StructType(
    Array
    (
      StructField("YEAR", IntegerType, true),
      StructField("QUARTER", IntegerType, true),
      StructField("MONTH", IntegerType, true),
      StructField("DAY_OF_MONTH", IntegerType, true),
      StructField("DAY_OF_WEEK", IntegerType, true),
      StructField("AIRLINE_ID", IntegerType, true),
      StructField("ORIGIN_AIRPORT_ID", IntegerType, true),
      StructField("DEST_AIRPORT_ID", IntegerType, true),
      StructField("CRS_DEP_TIME", IntegerType, true),
      StructField("DEP_DELAY", FloatType, true),
      StructField("DEP_DELAY_NEW", FloatType, true),
      StructField("CRS_ARR_TIME", IntegerType, true),
      StructField("CANCELLED", FloatType, true),
      StructField("DISTANCE", FloatType, true),
      StructField("DISTANCE_GROUP", IntegerType, true)
    )
  )

  //create DataFrame from csv
  val dataFrame: sql.DataFrame = sparkSession.sqlContext.read
    .format("com.databricks.spark.csv")
    .schema(customSchema)
    //.option("nullValue", "null")
    //.option("treatEmptyValuesAsNulls", "true")
    .option("header", "true")
    .option("delimiter", ",")
    .option("escape", "\"")
    .load("./data/Flight2017_01-02.csv")

  def timeToHours: (Int => Int) = p => p / 100
  val toHoursUDF = udf(timeToHours)
  val mappedFrame: sql.DataFrame = dataFrame
    .withColumn("CRS_DEP_TIME", toHoursUDF(dataFrame("CRS_DEP_TIME")))
    .withColumn("CRS_ARR_TIME", toHoursUDF(dataFrame("CRS_ARR_TIME")))

    .cache()

  val mappedFrameNoCancelled: sql.DataFrame = mappedFrame.filter(mappedFrame("CANCELLED") === 0f)






}
