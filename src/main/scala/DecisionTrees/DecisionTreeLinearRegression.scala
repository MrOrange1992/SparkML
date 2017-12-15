package DecisionTrees

import org.apache.log4j._
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.udf


object DecisionTreeLinearRegression
{

  def main(args: Array[String]): Unit =
  {
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)
    // Use new SparkSession interface in Spark 2.0
    val spark = SparkSession.builder.appName("DecisionTreeLinearRegression").master("local[*]").getOrCreate()

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

    val players = lines.map(apiWrapper.mappPlayerStats).toDS().cache()
    //val players = lines.map(mapper).toDS()//.cache()
    //players.printSchema()
    //players.show()

    // summary of points per game of dataset
    //players.describe("pointsPG").show()



    //EXAMPLES
    //------------------------------------------------------------------------------------------------------------------
    //Player LeBron James
    //players.filter(players("lastName") === "James").show()

    //Team Cleveland Cavaliers
    //players.filter(players("team") === "Cavaliers").show()

    //Position Small Forward sorted by points per game
    //players.filter($"position" === "SF").sort(- $"pointsPG").show()

    //Average stats by position
    //players.groupBy($"position").avg("pointsPG", "assistsPG", "reboundsPG").show()
    //------------------------------------------------------------------------------------------------------------------



    //------------------------------------------------------------------------------------------------------------------
    /*
        LINEAR REGRESSION

        see if there is a connection between:
          minutes played per game,
          free throw / field goal percentage
          and points per game

            label:      pointsPG
            features:   minSecPG, FtPct, FgPct
     */


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


    //summary / evaluation of trained model
    //--------------------------------------------------
    println(s"Coefficients: ${lrModel.coefficients} \nIntercept: ${lrModel.intercept}")

    val trainingSummary = lrModel.summary

    //println(s"numIterations: ${trainingSummary.totalIterations}")
    //println(s"objectiveHistory: ${trainingSummary.objectiveHistory.toList}")

    //trainingSummary.residuals.show()

    println(s"RMSE: ${trainingSummary.rootMeanSquaredError}")
    println(s"MSE: ${trainingSummary.meanSquaredError}")
    println(s"r2: ${trainingSummary.r2}")
    //--------------------------------------------------


    //test the model
    val predictions = lrModel.transform(testData)
    predictions.show()

    //show residuals
    //predictions.select(($"label" - $"prediction").as("residuals")).show()

    //calculate accuracy of predictions
    val evaluator = new BinaryClassificationEvaluator().setLabelCol("label").setRawPredictionCol("prediction").setMetricName("areaUnderROC")
    val accuracy = evaluator.evaluate(predictions)
    println(s"Accuracy: $accuracy")


    //------------------------------------------------------------------------------------------------------------------



    //stop session
    spark.stop()
  }

}
