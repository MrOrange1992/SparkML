package DecisionTrees

import org.apache.log4j._
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.sql.SparkSession


object DecisionTreeLinearRegression
{


  def main(args: Array[String]): Unit =
  {
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)
    // Use new SparkSession interface in Spark 2.0
    val spark = SparkSession.builder.appName("DecisionTreeLinearRegression").master("local[*]").getOrCreate()

    /*
      Currently Available
        2015-2016-regular
        2016-playoff
        2016-2017-regular
        2017-playoff
     */
    val apiWrapper = new WrapperMySportsAPI
    val s2016PlayerStats = apiWrapper.getStatsOfSeason("2016-playoff")

    // Convert http-request-stream List[String] to a DataSet
    import spark.implicits._
    val lines = spark.sparkContext.parallelize(s2016PlayerStats.tail)   //tail because first line is csv header

    val players = lines.map(apiWrapper.mapper).toDS().cache()
    //val players = lines.map(mapper).toDS()//.cache()
    //players.printSchema()
    //players.show()

    //Player Lebron James
    //players.filter(players("lastName") === "James").show()

    //Team Cleveland Cavaliers
    //players.filter(players("team") === "Cavaliers").show()

    //Position Small Forward sorted by points per game
    //players.filter($"position" === "SF").sort(- $"pointsPG").show()

    //Average stats by position
    //players.groupBy($"position").avg("pointsPG", "assistsPG", "reboundsPG").show()



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
    val lrData = players.select($"pointsPG".as("label"), $"minSecPG", $"fgPct", $"ftPct")

    //setting up features
    val assembler = new VectorAssembler().setInputCols(Array("minSecPG", "fgPct", "ftPct")).setOutputCol("features")

    //mapped dataset for Linear Regression
    val dataLR = assembler.transform(lrData).select("label", "features")

    //Linear Regression
    val lr = new LinearRegression()
    val lrModel = lr.fit(dataLR)

    println(s"Coefficients: ${lrModel.coefficients} Intercept: ${lrModel.intercept}")

    val trainingSummary = lrModel.summary

    println(s"numIterations: ${trainingSummary.totalIterations}")
    println(s"objectiveHistory: ${trainingSummary.objectiveHistory.toList}")

    trainingSummary.residuals.show()

    println(s"RMSE: ${trainingSummary.rootMeanSquaredError}")
    println(s"MSE: ${trainingSummary.meanSquaredError}")
    println(s"r2: ${trainingSummary.r2}")
    //------------------------------------------------------------------------------------------------------------------



    //stop session
    spark.stop()
  }

}
