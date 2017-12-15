package FlightAnalysis

import org.apache.log4j.{Level, Logger}

object FlightLinearRegression
{
  def main(args: Array[String]): Unit =
  {
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    //instance for mapper class for sparkSession
    val dataFrameMapper: FlightDataMapper = new FlightDataMapper

    val dataFrame = dataFrameMapper.dataFrame

    dataFrame.show()

  }
}
