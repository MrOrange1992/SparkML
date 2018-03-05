package FlightAnalysis

import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.sql.functions._
import co.theasi.plotly
import co.theasi.plotly.{Plot, writer}
import co.theasi.plotly._




object FD_LinReg_ML
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

    def num2bolNum: (Float => Double) = v => { if (v > 20) 1.0 else 0.0 }

    val bool2int_udf = udf(num2bolNum)

    val convertLabel = expandedFrame
      .withColumn("label", bool2int_udf(dataFrame("DEP_DELAY")))

    convertLabel.describe().show()

    //points per game as label
    val lrData = convertLabel
      .select(
        convertLabel("label"),
        //expandedFrame("YEAR"),
        //expandedFrame("QUARTER"),
        //expandedFrame("MONTH"),
        convertLabel("DAY_OF_MONTH"),
        convertLabel("DAY_OF_WEEK"),
        convertLabel("AIRLINE_ID"),
        convertLabel("ORIGIN_AIRPORT_ID"),
        convertLabel("DEST_AIRPORT_ID"),
        convertLabel("CRS_DEP_TIME"),
        convertLabel("DEP_DELAY_NEW"),
        convertLabel("CRS_ARR_TIME"),
        convertLabel("DISTANCE_GROUP"))

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
    //predictions.filter(predictions("label") === 1f).show()

    //show residuals
    //predictions.select(($"label" - $"prediction").as("residuals")).show()

    //calculate accuracy of predictions
    val evaluator = new BinaryClassificationEvaluator().setLabelCol("label").setRawPredictionCol("prediction").setMetricName("areaUnderROC")
    val accuracy = evaluator.evaluate(predictions)
    println(s"Accuracy: $accuracy")

    //------------------------------------------------------------------------------------------------------------------




    //PLOTLY
    //------------------------------------------------------------------------------------------------------------------

    import dataFrameMapper.sparkSession.implicits._

    //val plotData = predictions.rdd.toDS()//.sort($"_1").withColumn("label", $"_1").withColumn("prediction", $"_2")

    //plotData.describe().show()

    val evalFrame = predictions.filter(predictions("label") === 1f && predictions("prediction") >= 0.5)//randomSplit(Array(0.5, 0.5))(0)

    val xs = 0 until 500


    implicit val y1: Array[Double] = evalFrame.select($"label").rdd.map(_(0).toString.toDouble).collect().take(500)
    implicit val y2: Array[Double] = evalFrame.select($"prediction").rdd.map(_(0).toString.toDouble).collect().take(500)



    // Options common to traces
    val commonOptions = ScatterOptions().mode(ScatterMode.Marker).marker(MarkerOptions().size(8).lineWidth(1))


    // The plot itself
    val plot = Plot()
      .withScatter(xs, y1, commonOptions.name("Label"))
      .withScatter(xs, y2, commonOptions.name("Prediction"))



    draw(plot, "FD_LinReg_ML", writer.FileOptions(overwrite=true))


  }
}
