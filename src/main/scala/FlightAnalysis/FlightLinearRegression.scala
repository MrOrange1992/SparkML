package FlightAnalysis

import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.sql.functions._




object FlightLinearRegression
{
  def main(args: Array[String]): Unit =
  {
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    //instance for mapper class for sparkSession
    val dataFrameMapper: FlightDataMapper = new FlightDataMapper

    val dataFrame = dataFrameMapper.dataFrame


    //val expandedFrame = filteredFrame.withColumn("SUM_DELAY", abs(filteredFrame("DEP_DELAY")) + abs(filteredFrame("ARR_DELAY")))


    dataFrame.show()

    // TODO: take new csv with cancelled field and filter cancelled flights before evaluation

/*
    //points per game as label
    val lrData = players.select($"pointsPG".as("label"), $"position", $"gamesPlayed", $"minSecPG", $"fgPct", $"ftPct")

    //setting up features
    val assembler = new VectorAssembler().setInputCols(Array("position", "gamesPlayed", "minSecPG", "fgPct", "ftPct")).setOutputCol("features")

    //mapped dataset for Linear Regression
    val dataLR = assembler.transform(lrData).select("label", "features")

    //splitting data into training data and test data
    val splitData = dataLR.randomSplit(Array(0.7, 0.3))
    val trainingData = splitData(0)
    val testData = splitData(1)

    //Linear Regression model
    val lr = new LinearRegression()

    //train the model
    val lrModel = lr.fit(trainingData)
    */

  }
}
