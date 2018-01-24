package FlightAnalysis

import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
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

    val dataFrame = dataFrameMapper.mappedFrameNoCancelled

    val expandedFrame = dataFrame//.withColumn("SUM_DELAY", dataFrame("DEP_DELAY") + dataFrame("ARR_DELAY"))

    //dataFrame.show()


    //points per game as label
    val lrData = expandedFrame
      .select(
        expandedFrame("DEP_DELAY").as("label"),
        //expandedFrame("YEAR"),
        //expandedFrame("QUARTER"),
        //expandedFrame("MONTH"),
        expandedFrame("DAY_OF_MONTH"),
        expandedFrame("DAY_OF_WEEK"),
        expandedFrame("AIRLINE_ID"),
        expandedFrame("ORIGIN_AIRPORT_ID"),
        expandedFrame("DEST_AIRPORT_ID"),
        expandedFrame("CRS_DEP_TIME"),
        expandedFrame("DEP_DELAY_NEW"),
        expandedFrame("CRS_ARR_TIME"),
        expandedFrame("DISTANCE_GROUP"))

    lrData.groupBy(lrData("DEST_AIRPORT_ID")).count().describe().show()

    //lrData.describe().show()

    //setting up features
    val assembler = new VectorAssembler()
      .setInputCols(Array(
        //"YEAR",
        //"QUARTER",
        //"MONTH",
        "DAY_OF_MONTH",
        "DAY_OF_WEEK",
        "AIRLINE_ID",
        "ORIGIN_AIRPORT_ID",
        "DEST_AIRPORT_ID",
        "CRS_DEP_TIME",
        "DEP_DELAY_NEW",
        "CRS_ARR_TIME",
        "DISTANCE_GROUP"
      )).setOutputCol("features")




    //mapped dataset for Linear Regression
    val dataLR = assembler.transform(lrData).select("label", "features")

    //splitting data into training data and test data
    val splitData = dataLR.randomSplit(Array(0.2, 0.8))
    val trainingData = splitData(0)
    val testData = splitData(1)

    trainingData.show()

    //Linear Regression model
    val lr = new LinearRegression()

    //train the model
    val lrModel = lr.fit(trainingData)


    //summary / evaluation of trained model
    //--------------------------------------------------
    println(s"Coefficients: ${lrModel.coefficients} \nIntercept: ${lrModel.intercept}")

    val trainingSummary = lrModel.summary

    //println(s"numIterations: ${trainingSummary.totalIterations}")
    //println(s"objectiveHistory: ${trainingSummary.objectiveHistory.toList}")

    //trainingSummary.residuals.show()

    println(s"RMSE: ${trainingSummary.rootMeanSquaredError}")
    println(s"MSE: ${trainingSummary.meanSquaredError}")
    println(s"r2: ${trainingSummary.r2}\n")
    //--------------------------------------------------


    //test the model
    val predictions = lrModel.transform(testData)
    predictions.filter(predictions("label") === 1f).show()

    //show residuals
    //predictions.select(($"label" - $"prediction").as("residuals")).show()

    //calculate accuracy of predictions
    val evaluator = new BinaryClassificationEvaluator().setLabelCol("label").setRawPredictionCol("prediction").setMetricName("areaUnderROC")
    val accuracy = evaluator.evaluate(predictions)
    println(s"Accuracy: $accuracy")

    //------------------------------------------------------------------------------------------------------------------

  }
}
