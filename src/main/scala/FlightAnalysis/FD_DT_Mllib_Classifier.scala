package FlightAnalysis


import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.DecisionTree
import org.apache.spark.sql.functions.udf




object FD_DT_Mllib_Classifier
{

  def main(args: Array[String]): Unit =
  {
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    //instance for mapper class for sparkSession
    val dataFrameMapper: FlightDataMapper = new FlightDataMapper

    val dataFrame = dataFrameMapper.mappedFrameNoCancelled

    val newFrame = dataFrame.filter(dataFrame("ORIGIN_STATE_ABR") === "CA")

    val renamedFrame = newFrame.withColumnRenamed("FL_DATE", "DATE")
    //renamedFrame.describe().show()

    val weatherFrame = dataFrameMapper.weatherFrame
    //weatherFrame.describe().show()

    val completeFrame = renamedFrame.join(weatherFrame, "DATE")
    //completeFrame.describe().show()

    val finishedFrame = completeFrame.select(
      "MONTH",
      "DAY_OF_MONTH",
      "DAY_OF_WEEK",
      "AIRLINE_ID",
      "ORIGIN_AIRPORT_ID",
      "DEST_AIRPORT_ID",
      "CRS_DEP_TIME",
      "DEP_DELAY",
      "DISTANCE_GROUP",
      "PRCP",
      "SNOW",
      "SNWD",
      "TAVG",
      "TMAX",
      "TMIN",
      "WESF",
      "WT01",
      "WT02",
      "WT03",
      "WT04",
      "WT05",
      "WT06",
      "WT07",
      "WT08",
      "WT11"
    )


    def num2bolNum: (Float => Double) = v => if (v > 35) 1.0 else 0.0

    val bool2int_udf = udf(num2bolNum)

    val convertLabel = finishedFrame
      .withColumn("IS_DELAYED", bool2int_udf(dataFrame("DEP_DELAY")))


    //converting to RDD for mllib model
    val expandedFrame = convertLabel.rdd //.withColumn("SUM_DELAY", dataFrame("DEP_DELAY") + dataFrame("ARR_DELAY"))

    //mapping to LabeledPoint for label / feature Vector
    val dtData = expandedFrame.map(row => LabeledPoint(
      row.getAs[Int]("IS_DELAYED"),
      Vectors.dense(
        row.getAs[Int]("MONTH"),
        row.getAs[Int]("DAY_OF_MONTH"),
        row.getAs[Int]("DAY_OF_WEEK"),
        row.getAs[Int]("AIRLINE_ID"),
        row.getAs[Int]("ORIGIN_AIRPORT_ID"),
        row.getAs[Int]("DEST_AIRPORT_ID"),
        row.getAs[Int]("CRS_DEP_TIME"),
        row.getAs[Int]("DISTANCE_GROUP"),
        row.getAs[Float]("PRCP"),
        row.getAs[Float]("SNOW"),
        row.getAs[Float]("SNWD"),
        row.getAs[Float]("TAVG"),
        row.getAs[Float]("TMAX"),
        row.getAs[Float]("TMIN"),
        row.getAs[Float]("WESF"),
        row.getAs[Int]("WT01"),
        row.getAs[Int]("WT02"),
        row.getAs[Int]("WT03"),
        row.getAs[Int]("WT04"),
        row.getAs[Int]("WT05"),
        row.getAs[Int]("WT06"),
        row.getAs[Int]("WT07"),
        row.getAs[Int]("WT08"),
        row.getAs[Int]("WT11")
      )))


    //split data for training and testing
    val splits = dtData.randomSplit(Array(0.5, 0.5))
    val (trainingData, testData) = (splits(0), splits(1))

    // Train a DecisionTree model.
    //  Empty categoricalFeaturesInfo indicates all features are continuous.
    val numClasses = 2
    val categoricalFeaturesInfo = Map[Int, Int]()
    val impurity = "gini"
    val maxDepth = 5
    val maxBins = 32

    //train the model
    val model = DecisionTree.trainClassifier(trainingData, numClasses, categoricalFeaturesInfo,
      impurity, maxDepth, maxBins)



    import dataFrameMapper.sparkSession.implicits._


    //get predictions from testData
    val labelAndPreds = testData.map(entry => (entry.label, model.predict(entry.features))).toDF()
    val correctCount = labelAndPreds.select("*").where("_1 = _2").count()
    val percentage: Float = correctCount.asInstanceOf[Float] / testData.count() * 100

    println(s"\nTotal: ${testData.count()}")
    println(s"Correct predictions: $correctCount")
    println(s"Percentage: $percentage")

    //string representation of created decision tree model
    println("\nLearned classification tree model:\n" + model.toDebugString)






    /*
    //PLOTLY
    //------------------------------------------------------------------------------------------------------------------

    import dataFrameMapper.sparkSession.sqlContext.implicits._

    val plotData = labelAndPreds.toDF().sort($"_1").withColumn("label", $"_1").withColumn("prediction", $"_2")

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



    draw(plot, "FD_DT_10-90_Part", writer.FileOptions(overwrite=true))

    */



    dataFrameMapper.sparkSession.stop()

  }

}
