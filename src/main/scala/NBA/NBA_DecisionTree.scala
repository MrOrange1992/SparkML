package NBA

import breeze.numerics.abs
import co.theasi.plotly.{MarkerOptions, Plot, ScatterMode, ScatterOptions, draw, writer}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.DecisionTree
import org.apache.spark.sql.SparkSession

import scala.math.round

object NBA_DecisionTree
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
    val lines = spark.sparkContext.parallelize(s2016PlayerStats.tail)   //tail because first line is csv header

    val players = lines.flatMap(apiWrapper.mappPlayerStats)


    val dtData = players.map(player => LabeledPoint(
      player.pointsPG, // Get target value
      // Map feature indices to values
      Vectors.dense(
        player.gamesPlayed,
        player.minSecPG,
        player.fgPct,
        player.ftPct,
        player.position,
        player.weight,
        player.height
      )
    ))

    val splits = dtData.randomSplit(Array(0.5, 0.5))
    val (trainingData, testData) = (splits(0), splits(1))

    // Train a DecisionTree model.
    //  Empty categoricalFeaturesInfo indicates all features are continuous.
    val categoricalFeaturesInfo = Map[Int, Int]()
    val impurity = "variance"
    val maxDepth = 25
    val maxBins = 32

    val model = DecisionTree.trainRegressor(trainingData, categoricalFeaturesInfo, impurity,
      maxDepth, maxBins)

    // Evaluate model on test instances and compute test error
    val labelAndPreds = testData.map { point =>
      val prediction = model.predict(point.features)
      (BigDecimal(point.label).setScale(1, BigDecimal.RoundingMode.HALF_UP).toDouble,
        BigDecimal(prediction).setScale(1, BigDecimal.RoundingMode.HALF_UP).toDouble)
    }


    //val testErr = labelAndPreds.filter(r => r._1 != r._2).count().toDouble / testData.count()
    val avgErr = labelAndPreds.map(r => abs(r._1 - r._2)).sum() / testData.count()


    println("Average Error = " + avgErr)
    println("\nLearned classification tree model:\n" + model.toDebugString)


    labelAndPreds.toDS().show()


    //PLOTLY
    //------------------------------------------------------------------------------------------------------------------

    val plotData = labelAndPreds.toDS().sort($"_1").withColumn("label", $"_1").withColumn("prediction", $"_2")

    plotData.describe().show()

    val xs = 0 until 300


    implicit val y1: Array[Double] = plotData.select($"label").rdd.map(_(0).toString.toDouble).collect()
    implicit val y2: Array[Double] = plotData.select($"prediction").rdd.map(_(0).toString.toDouble).collect()



    // Options common to traces
    val commonOptions = ScatterOptions().mode(ScatterMode.Marker).marker(MarkerOptions().size(8).lineWidth(1))


    // The plot itself
    val plot = Plot()
      .withScatter(xs, y1, commonOptions.name("Label"))
      .withScatter(xs, y2, commonOptions.name("Prediction"))



    draw(plot, "NBA_DT_16-17_50-50_All", writer.FileOptions(overwrite=true))


    //stop session
    spark.stop()
  }

}
