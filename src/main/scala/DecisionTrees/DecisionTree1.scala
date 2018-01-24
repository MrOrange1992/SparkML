package DecisionTrees

import breeze.numerics.{abs}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.DecisionTree
import org.apache.spark.sql.SparkSession
import scala.math.round

object DecisionTree1
{
  def main(args: Array[String]): Unit =
  {
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)
    // Use new SparkSession interface in Spark 2.0
    val spark = SparkSession.builder.appName("DecisionTree1").master("local[*]").getOrCreate()

    /*
      Currently Available (Nov 2017)
        2015-2016-regular
        2016-playoff
        2016-2017-regular
        2017-playoff
     */
    val apiWrapper = new WrapperMySportsAPI
    val s2016PlayerStats = apiWrapper.getPlayerStatsOfSeason("2016-playoff")

    // Convert http-request-stream List[String] to a DataSet
    import spark.implicits._
    val lines = spark.sparkContext.parallelize(s2016PlayerStats.tail)   //tail because first line is csv header

    val players = lines.map(apiWrapper.mappPlayerStats)

    val dtData = players.map(player => LabeledPoint(
      player.pointsPG, // Get target value
      // Map feature indices to values
      Vectors.dense(player.gamesPlayed, player.minSecPG/*, player.fgPct, player.ftPct, player.position*/)
    ))

    val splits = dtData.randomSplit(Array(0.7, 0.3))
    val (trainingData, testData) = (splits(0), splits(1))

    // Train a DecisionTree model.
    //  Empty categoricalFeaturesInfo indicates all features are continuous.
    val categoricalFeaturesInfo = Map[Int, Int]()
    val impurity = "variance"
    val maxDepth = 5
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


    // Save and load model
    //model.save(sc, "target/tmp/myDecisionTreeClassificationModel")
    //val sameModel = DecisionTreeModel.load(sc, "target/tmp/myDecisionTreeClassificationModel")

    //stop session
    spark.stop()
  }

}
