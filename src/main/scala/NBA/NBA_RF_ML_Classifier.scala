package NBA

import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.{DecisionTreeClassifier, RandomForestClassifier}
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.regression.{DecisionTreeRegressionModel, DecisionTreeRegressor}
import org.apache.spark.sql
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.types._
import org.apache.spark.sql.{SparkSession, functions}

import scala.io.Source

object NBA_RF_ML_Classifier
{
  def main(args: Array[String]): Unit = {
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.OFF)
    // Use new SparkSession interface in Spark 2.0
    val spark = SparkSession.builder.appName("NBA_RandomForest").master("local[*]").getOrCreate()

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

    val teamGameLogs = new TeamGameLogs

    val seasonName: String = "2016-2017-regular"

    val teamLogs = apiWrapper.getHttpRequest(seasonName + teamGameLogs.gameLogsParam + teamGameLogs.teamNameRequest)

    // Convert http-request-stream List[String] to a DataSet
    import spark.implicits._
    val lines = spark.sparkContext.parallelize(teamLogs.tail) //tail because first line is csv header

    val gameLogs = lines.flatMap(teamGameLogs.mappGameLog).toDS()

    def mapType: (Float => Double) = v => v.toDouble

    val bool2int_udf = udf(mapType)

    val dataFrame = gameLogs.withColumn("WinLoss", bool2int_udf($"WinLoss")).na.fill(0)

    val dataSet = dataFrame.withColumn("label", $"WinLoss")

    val featureArray = Array(
      "Fg2PtMade",
      "Fg2PtPct",
      "Fg3PtMade",
      "Fg3PtPct",
      "FgMade",
      "FgPct",
      "FtAtt",
      "FtMade",
      "FtPct",
      "OffReb",
      "DefReb",
      "Ast",
      "Pts",
      "Tov",
      "Stl",
      "Blk",
      "BlkAgainst",
      "PtsAgainst",
      "Fouls",
      "FoulsDrawn"
      //"PlusMinus"
    )

    //setting up features
    val assembler = new VectorAssembler().setInputCols(featureArray).setOutputCol("features")

    //mapped dataset for Linear Regression
    val rfData = assembler.transform(dataSet).select("label", "features")

    val Array(trainingData, testData) = rfData.randomSplit(Array(0.3, 0.7))

    //------------------------------------------------------------------------------------------------------------------

    /*
        Comparison
          -> Overfitting (all features)
                              -> Single Tree slightly better

        7 features: FgPct,FtAtt,Ast,Pts,Tov,Stl,Blk
          -> Single tree percentage deviates around 2 percent ~68 - 70 pct
          -> Forest(20) also deviates, but slightly better percentage overall
          -> Forest(100) much less deviation ~0.3 percentage wise same as Forest(20)




        TODO; make data of multiple runs for decision tree and then forest and make plot with results to compare stability

     */



    // Decision tree classifier model
    //val model = new DecisionTreeClassifier()
    // .setMaxDepth(6)
    // .setMaxBins(200)

    // Random forest classifier model
    val model = new RandomForestClassifier()
      .setNumTrees(100)
      .setFeatureSubsetStrategy("all")
      .setMaxDepth(6)
      .setMaxBins(200)

    // train the model
    val trainedModel = model.fit(trainingData)

    val featureImportances = trainedModel.featureImportances

    val featureCoefficientMap = (featureArray zip featureImportances.toArray).map(entry => entry._1 -> entry._2).toMap

    val coefficientFrame = featureCoefficientMap.toSeq.toDF("name", "value").orderBy($"value".desc)

    //println("Feature Importances:")

    //coefficientFrame.show()

    //println("Learned classification tree model:\n" + trainedModel.toDebugString)


    val predictions = trainedModel.transform(testData)

    //show residuals
    //predictions.select($"label", $"prediction").describe().show()

    val allCount = predictions.count()
    val correctPredictions = predictions.filter($"label" === $"prediction").count()
    val percentage = (correctPredictions.asInstanceOf[Float] / allCount) * 100

    //println(s"Total Data points of training set: $allCount")
    //println(s"Correct Predictions: $correctPredictions")
    //println(s"Percentage: $percentage")
    //println(percentage)
    //------------------------------------------------------------------------------------------------------------------


    //create plotly scatter plots
    val plot:Plot = new Plot()

    val stabilitySampleFrame: sql.DataFrame = spark.sqlContext.read
      .format("com.databricks.spark.csv")
      .schema(StructType(Array(StructField("treeSamples", FloatType, true), StructField("forestSamples", FloatType, true))))
      .option("header", "true")
      .option("delimiter", ",")
      .load("./dataFiles/CompareStability.csv")

    stabilitySampleFrame.describe().show()


    val treeData = stabilitySampleFrame.select($"treeSamples").rdd.map(_(0).toString.toDouble).collect()
    val forestData = stabilitySampleFrame.select($"forestSamples").rdd.map(_(0).toString.toDouble).collect()


    plot.makeScattePlotDoubleTrace("StabilityTest", Range(1,50), treeData, "DecisionTree", forestData, "Random Forest")


  }

}
