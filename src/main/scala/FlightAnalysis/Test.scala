package FlightAnalysis

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions._


object Test
{
  def main(args: Array[String]): Unit =
  {
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    //instance for mapper class for sparkSession
    val dataFrameMapper: FlightDataMapper = new FlightDataMapper

    val dataFrame = dataFrameMapper.mappedFrameNoCancelled


    dataFrame.groupBy("ORIGIN_STATE_ABR").count().orderBy(desc("count")).show()

  }

}
