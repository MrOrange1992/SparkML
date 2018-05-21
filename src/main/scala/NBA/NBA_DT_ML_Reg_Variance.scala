package NBA

import co.theasi.plotly.{draw, MarkerOptions, Plot, ScatterMode, ScatterOptions, writer}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.DecisionTreeClassificationModel
import org.apache.spark.ml.evaluation.{MulticlassClassificationEvaluator, RegressionEvaluator}
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.regression.{DecisionTreeRegressionModel, DecisionTreeRegressor}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.sql.{SparkSession, functions}

object NBA_DT_ML_Reg_Variance
{


  def main(args: Array[String]): Unit = {
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)
    // Use new SparkSession interface in Spark 2.0
    val spark = SparkSession.builder.appName("NBA_DecisionTree").master("local[*]").getOrCreate()

    /*
        Currently Available (Nov 2017)
          2015-2016-regular
          2016-playoff
          2016-2017-regular
          2017-playoff
          2017-2018-regular
          2018-playoff
       */
    val apiWrapper = new WrapperMySportsAPI

    val cumulativePlayerStats = new CumulativePlayerStats

    val seasonName: String = "2016-2017-regular"

    val s2016PlayerStats = apiWrapper.getHttpRequest(seasonName + cumulativePlayerStats.cumulativeString + cumulativePlayerStats.playerStatsRequest)

    // Convert http-request-stream List[String] to a DataSet
    import spark.implicits._
    val lines = spark.sparkContext.parallelize(s2016PlayerStats.tail) //tail because first line is csv header

    val players = lines.flatMap(cumulativePlayerStats.mappPlayerStats).toDS()
      .select(
        "pointsPG",
        "weight",
        "height",
        "position",
        "fgPct",
        "ftPct",
        "minSecPG",
        "gamesPlayed"
      )

    val featureArray = Array(
      "weight",
      "height",
      "position",
      "fgPct",
      "ftPct",
      "minSecPG",
      "gamesPlayed"
    )

    //players.show()

    val playerFrame = players.withColumn("label", players("pointsPG"))

    val featureVector = new VectorAssembler()
      .setInputCols(featureArray)
      .setOutputCol("features")

    val dtDataFrame = featureVector.transform(playerFrame).select("label", "features")

    // Split the data into training and test sets
    val Array(trainingData, testData) = dtDataFrame.randomSplit(Array(0.5, 0.5))

    // train the model
    val dt = new DecisionTreeRegressor()
      .setLabelCol("label")
      .setFeaturesCol("features")
      .setMaxDepth(3)
      .setMaxBins(100)

    val model = dt.fit(trainingData)

    //compare feature importances
    val featureImportances = model.featureImportances
    val featureCoefficientMap = (featureArray zip featureImportances.toArray).map(entry => entry._1 -> entry._2).toMap
    val coefficientFrame = featureCoefficientMap.toSeq.toDF("name", "value").orderBy($"value".desc)

    println("Feature Importances:")
    coefficientFrame.show()

    //print model structure
    println("Tree structure:\n" + model.toDebugString)

    // Make predictions.
    val predictions = model.transform(testData)

    // Select example rows to display.
    val residuals = predictions.select("prediction", "label", "features")

    residuals.describe().show()

    //residuals.show()


    val accuracy = residuals.withColumn("error", functions.abs(residuals("label") - residuals("prediction")))

    accuracy.describe().show()

    //accuracy.show()


    //PLOTLY
    //------------------------------------------------------------------------------------------------------------------

    val plotData = residuals.sort("prediction")

    val xs = 0 until 300


    implicit val y1: Array[Double] = plotData.select($"label").rdd.map(_ (0).toString.toDouble).collect()
    implicit val y2: Array[Double] = plotData.select($"prediction").rdd.map(_ (0).toString.toDouble).collect()

    // Options common to traces
    val commonOptions = ScatterOptions().mode(ScatterMode.Marker).marker(MarkerOptions().size(8).lineWidth(1))

    // The plot itself
    val plot = Plot()
      .withScatter(xs, y1, commonOptions.name("Label"))
      .withScatter(xs, y2, commonOptions.name("Prediction"))


    draw(plot, "PointsPerGameSortedByPrediction", writer.FileOptions(overwrite = true))


  }

}
