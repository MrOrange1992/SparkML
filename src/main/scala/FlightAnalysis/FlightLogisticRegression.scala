package FlightAnalysis

import breeze.numerics.abs
import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.sql.functions.udf

object FlightLogisticRegression
{
  def main(args: Array[String]): Unit =
  {
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    //instance for mapper class for sparkSession
    val dataFrameMapper: FlightDataMapper = new FlightDataMapper

    val mapperFrame = dataFrameMapper.mappedFrameNoCancelled

    val newFrame = mapperFrame.filter(mapperFrame("ORIGIN_STATE_ABR") === "CA")

    val renamedFrame = newFrame.withColumnRenamed("FL_DATE", "DATE")


    val weatherFrame = dataFrameMapper.weatherFrame

    val joinedFrame = renamedFrame.join(weatherFrame, "DATE")


    def num2bolNum: (Float => Int) = v => { if (v > 35) 1 else 0 }

    val bool2int_udf = udf(num2bolNum)

    val dataFrame = joinedFrame.withColumn("IS_DELAYED", bool2int_udf(joinedFrame("DEP_DELAY"))).na.fill(0)

    //dataFrame.rdd.map(row => row.map)


    //points per game as label
    val lrData = dataFrame
      .select(
        dataFrame("IS_DELAYED").as("label"),
        //expandedFrame("YEAR"),
        //expandedFrame("QUARTER"),
        //expandedFrame("MONTH"),
        dataFrame("DAY_OF_MONTH"),
        dataFrame("DAY_OF_WEEK"),
        dataFrame("AIRLINE_ID"),
        dataFrame("ORIGIN_AIRPORT_ID"),
        dataFrame("DEST_AIRPORT_ID"),
        dataFrame("CRS_DEP_TIME"),
        dataFrame("DISTANCE_GROUP"),
        dataFrame("PRCP"),
        dataFrame("SNOW"),
        dataFrame("SNWD"),
        dataFrame("TAVG"),
        dataFrame("TMAX"),
        dataFrame("TMIN"),
        dataFrame("WESF"),
        dataFrame("WT01"),
        dataFrame("WT02"),
        dataFrame("WT03"),
        dataFrame("WT04"),
        dataFrame("WT05"),
        dataFrame("WT06"),
        dataFrame("WT07"),
        dataFrame("WT08"),
        dataFrame("WT11")
      )




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
        "DISTANCE_GROUP",
        "PRCP",
        "SNOW",
        "SNWD",
        "TAVG",
        "TMAX",
        "TMIN",
        "WESF",
        "WT01",
        "WT02",
        "WT03",
        "WT04",
        "WT05",
        "WT06",
        "WT07",
        "WT08",
        "WT11"
      )).setOutputCol("features")





    //mapped dataset for Linear Regression
    val dataLR = assembler.transform(lrData).select("label", "features")

    //splitting data into training data and test data
    val splitData = dataLR.randomSplit(Array(0.2, 0.8))
    val trainingData = splitData(0)
    val testData = splitData(1)

    trainingData.show()

    //Linear Regression model
    val lr = new LogisticRegression()

    //train the model
    val lrModel = lr.fit(trainingData)


    //summary / evaluation of trained model
    //--------------------------------------------------
    println(s"Coefficients: ${lrModel.coefficients} \nIntercept: ${lrModel.intercept}")

    val trainingSummary = lrModel.summary

    //println(s"numIterations: ${trainingSummary.totalIterations}")
    //println(s"objectiveHistory: ${trainingSummary.objectiveHistory.toList}")

    //trainingSummary.residuals.show()

    //println(s"RMSE: ${trainingSummary.rootMeanSquaredError}")
    //println(s"MSE: ${trainingSummary.meanSquaredError}")
    //println(s"r2: ${trainingSummary.r2}\n")
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



  }

}
