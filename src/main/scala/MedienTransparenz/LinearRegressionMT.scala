package MedienTransparenz

import MedienTransparenz.QuarterSpendings.MediaDataRow
import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.sql._
import org.apache.log4j._
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.regression.LinearRegressionWithSGD
import org.apache.spark.mllib.optimization.SquaredL2Updater

import scala.util.Try

object LinearRegressionMT
{


  def mappData(line: String): String =
  {
    try
    {
      //val fields = line.split(',')
      val fields = line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)")

      s"${fields(1)}, ${Try { fields(5).toInt }.getOrElse(0)}" //handle null fields
    }
    catch { case e: Exception => throw new Exception(s"\nError @ mapping data lines. \nCorrupt data in line:    $line")}

  }

  /** Our main function where the action happens */
  def main(args: Array[String]) {
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Create a SparkContext using every core of the local machine
    val sc = new SparkContext("local[*]", "LinearRegression")

    val lines = sc.textFile("./data/MedienTransparenz-20123-20171.csv")

    // map raw data to RDD
    val mediaRDD = lines.map(mappData)

    //val trainingLines = sc.textFile("../regression.txt")


    // Convert input data to LabeledPoints for MLLib
    val trainingData = mediaRDD.map(LabeledPoint.parse).cache()

    //val trainingLines = sc.textFile("../regression.txt")


    val testData = mediaRDD.map(LabeledPoint.parse)


    // Now we will create our linear regression model

    val algorithm = new LinearRegressionWithSGD()
    algorithm.optimizer
      .setNumIterations(100)
      .setStepSize(1.0)
      .setUpdater(new SquaredL2Updater())
      .setRegParam(0.01)


    val model = algorithm.run(trainingData)

    trainingData foreach println

    /*

    // Predict values for our test feature data using our linear regression model
    val predictions = model.predict(testData.map(_.features))

    // Zip in the "real" values so we can compare them
    val predictionAndLabel = predictions.zip(testData.map(_.label))

    // Print out the predicted and actual values for each point
    for (prediction <- predictionAndLabel) {
      println(prediction)
    }
    */
  }
}