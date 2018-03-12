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


  def main(args: Array[String]): Unit =
  {
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
       */
    val apiWrapper = new WrapperMySportsAPI
    val s2016PlayerStats = apiWrapper.getPlayerStatsOfSeason("2016-2017-regular")

    // Convert http-request-stream List[String] to a DataSet
    import spark.implicits._
    val lines = spark.sparkContext.parallelize(s2016PlayerStats.tail) //tail because first line is csv header

    val players = lines.flatMap(apiWrapper.mappPlayerStats).toDS()
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

    //players.show()

    val testSet = players.withColumn("label", players("pointsPG"))

    val vectorAssembler = new VectorAssembler()
      .setInputCols(Array(
        //"weight",
        //"height",
        //"position",
        "fgPct",
        //"ftPct",
        "minSecPG",
        "gamesPlayed"
      ))
      .setOutputCol("features")


    // Split the data into training and test sets (30% held out for testing).
    val Array(trainingData, testData) = testSet.randomSplit(Array(0.7, 0.3))

    // Train a DecisionTree model.
    val dt = new DecisionTreeRegressor()
      .setLabelCol("label")
      .setFeaturesCol("features")
      .setMaxDepth(6)
      .setMaxBins(100)

    // Chain indexer and tree in a Pipeline.
    val pipeline = new Pipeline()
      .setStages(Array(vectorAssembler, dt))

    // Train model. This also runs the indexer.
    val model = pipeline.fit(trainingData)

    val treeModel = model.stages(1).asInstanceOf[DecisionTreeRegressionModel]
    println("Learned classification tree model:\n" + treeModel.toDebugString)

    // Make predictions.
    val predictions = model.transform(testData)

    // Select example rows to display.
    val residuals = predictions.select("prediction", "label", "features")

    residuals.describe().show()

    residuals.show()


    val accuracy = residuals.withColumn("acurracy", functions.abs(residuals("label") - residuals("prediction")))

    accuracy.describe().show()

    //accuracy.show()



    //PLOTLY
    //------------------------------------------------------------------------------------------------------------------

    /*
    val xs = 0 until 300


    implicit val y1: Array[Double] = residuals.select($"label").rdd.map(_(0).toString.toDouble).collect()
    implicit val y2: Array[Double] = residuals.select($"prediction").rdd.map(_(0).toString.toDouble).collect()

    // Options common to traces
    val commonOptions = ScatterOptions().mode(ScatterMode.Marker).marker(MarkerOptions().size(8).lineWidth(1))

    // The plot itself
    val plot = Plot()
      .withScatter(xs, y1, commonOptions.name("Label"))
      .withScatter(xs, y2, commonOptions.name("Prediction"))


    draw(plot, "NBA_DT_ML", writer.FileOptions(overwrite=true))

    */
  }

}
