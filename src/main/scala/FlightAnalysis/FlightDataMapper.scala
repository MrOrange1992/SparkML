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
  val customSchemaFlight = StructType(
    Array
    (
      StructField("YEAR", IntegerType, true),
      StructField("QUARTER", IntegerType, true),
      StructField("MONTH", IntegerType, true),
      StructField("DAY_OF_MONTH", IntegerType, true),
      StructField("DAY_OF_WEEK", IntegerType, true),
      StructField("FL_DATE", StringType, true),
      StructField("AIRLINE_ID", IntegerType, true),
      StructField("ORIGIN_AIRPORT_ID", IntegerType, true),
      StructField("ORIGIN_STATE_ABR", StringType, true),
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
    .schema(customSchemaFlight)
    .option("header", "true")
    .option("delimiter", ",")
    .option("escape", "\"")
    .load("./dataFiles/FD_2017.csv")

  def timeToHours: (Int => Int) = p => p / 100
  val toHoursUDF = udf(timeToHours)
  val mappedFrame: sql.DataFrame = dataFrame
    .withColumn("CRS_DEP_TIME", toHoursUDF(dataFrame("CRS_DEP_TIME")))
    .withColumn("CRS_ARR_TIME", toHoursUDF(dataFrame("CRS_ARR_TIME")))

    .cache()

  val mappedFrameNoCancelled: sql.DataFrame = mappedFrame.filter(mappedFrame("CANCELLED") === 0f)


  /*

  "STATION","NAME","DATE","PRCP","SNOW","SNWD","TAVG","TMAX","TMIN","WESF","WT01","WT02","WT03","WT04","WT05","WT06","WT07","WT08","WT11"
  "USR0000CTHO","THOMES CREEK CALIFORNIA CA US","2017-01-01",0,0,0,"5.1","10.0","1.1",0,0,0,0,0,0,0,0,0,

  STATION,    NAME,                         DATE,         AWND, PRCP, SNOW, SNWD, TAVG,   TMAX, TMIN, WESF, WT01,WT02,WT03,WT04,WT05,WT06,WT07,WT08,WT10,WT11

  USR0000CTHO,THOMES CREEK CALIFORNIA CA US,2017-01-01,   0,    0,    0,    0,    41,     50,   34,    0,    0,    0,  0,   0,    0,  0,    0,   0,  0,   0


   */

  val customSchemaWeather = StructType(
    Array
    (
      StructField("STATION", StringType, true),
      StructField("NAME", StringType, true),
      StructField("DATE", StringType, true),
      StructField("AWND", FloatType, true),
      StructField("PRCP", FloatType, true),
      StructField("SNOW", FloatType, true),
      StructField("SNWD", FloatType, true),
      StructField("TAVG", FloatType, true),
      StructField("TMAX", FloatType, true),
      StructField("TMIN", FloatType, true),
      StructField("WESF", FloatType, true),
      StructField("WT01", IntegerType, true),
      StructField("WT02", IntegerType, true),
      StructField("WT03", IntegerType, true),
      StructField("WT04", IntegerType, true),
      StructField("WT05", IntegerType, true),
      StructField("WT06", IntegerType, true),
      StructField("WT07", IntegerType, true),
      StructField("WT08", IntegerType, true),
      StructField("WT10", IntegerType, true),
      StructField("WT11", IntegerType, true)
    )
  )

  val weatherFrame: sql.DataFrame = sparkSession.sqlContext.read
    .format("com.databricks.spark.csv")
    .schema(customSchemaWeather)
    .option("header", "true")
    .option("delimiter", ",")
    .option("escape", "\"")
    .load("./dataFiles/WData_2017.csv")

  }
