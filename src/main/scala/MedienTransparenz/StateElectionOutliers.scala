package MedienTransparenz

import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.sql.functions.{asc, desc, format_number}
import org.apache.spark.sql.functions.udf

object StateElectionOutliers
{
  def main(args: Array[String]): Unit =
  {
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    //instance for mapper class for sparkSession
    val dataFrameMapper: DataFrameMapper = new DataFrameMapper

    val dataFrame = dataFrameMapper.filterDF(true, true, false)

    val filteredSTMK = dataFrame.filter(dataFrame("federalState") === "AT-6")

    def periodToYear: (Int => Int) = p => p / 10
    def periodToMonth: (Int => Int) = p => p % 10
    val toYearUDF = udf(periodToYear)
    val toMonthUDF = udf(periodToMonth)

    val mappedFrame = filteredSTMK.select("period", "amount")
      .withColumn("year", toYearUDF(filteredSTMK("period")))
      .withColumn("month", toMonthUDF(filteredSTMK("period")))
      .drop("period")
    //mappedFrame.show()


    //LINEAR REGRESSION
    //------------------------------------------------------------------------------------------------------------------
    //points per game as label

    val mappedDF = mappedFrame.select(mappedFrame("amount").as("label"), mappedFrame("year"), mappedFrame("month"))

    //setting up features
    val assembler = new VectorAssembler().setInputCols(Array("year", "month")).setOutputCol("features")

    //mapped dataset for Linear Regression
    val dataLR = assembler.transform(mappedDF).select("label", "features")

    //Linear Regression model
    val lr = new LinearRegression()

    //train the model
    val lrModel = lr.fit(dataLR)

    //summary / evaluation of trained model
    //--------------------------------------------------
    println(s"Coefficients: ${lrModel.coefficients} \nIntercept: ${lrModel.intercept}")

    val trainingSummary = lrModel.summary

    //println(s"numIterations: ${trainingSummary.totalIterations}")
    //println(s"objectiveHistory: ${trainingSummary.objectiveHistory.toList}")

    trainingSummary.residuals.show()

    println(s"RMSE: ${trainingSummary.rootMeanSquaredError}")
    println(s"MSE: ${trainingSummary.meanSquaredError}")
    println(s"r2: ${trainingSummary.r2}")
    //--------------------------------------------------

    //------------------------------------------------------------------------------------------------------------------

  }
}
