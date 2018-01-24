package FlightAnalysis

import breeze.numerics.abs
import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.feature.{PCA, VectorAssembler}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.DecisionTree
import org.apache.spark.sql.functions.udf


object FlightDecisionTree
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

    //renamedFrame.show()

    val weatherFrame = dataFrameMapper.weatherFrame

    val completeFrame = renamedFrame.join(weatherFrame, "DATE")

    //completeFrame.show()

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


    //TODO: delay > 30min/40min
    def num2bolNum: (Float => Int) = v => { if (v > 35) 1 else 0 }

    val bool2int_udf = udf(num2bolNum)

    val convertLabel = finishedFrame.withColumn("IS_DELAYED", bool2int_udf(dataFrame("DEP_DELAY")))

    val expandedFrame = convertLabel.rdd //.withColumn("SUM_DELAY", dataFrame("DEP_DELAY") + dataFrame("ARR_DELAY"))


    val dtData = expandedFrame.map(row => LabeledPoint(
      row.getAs[Int]("IS_DELAYED"), // Get target value
      // Map feature indices to values
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



    val splits = dtData.randomSplit(Array(0.3, 0.7))
    val (trainingData, testData) = (splits(0), splits(1))

    // Train a DecisionTree model.
    //  Empty categoricalFeaturesInfo indicates all features are continuous.
    val numClasses = 2
    val categoricalFeaturesInfo = Map[Int, Int]()
    val impurity = "gini"
    val maxDepth = 5
    val maxBins = 32

    val model = DecisionTree.trainClassifier(trainingData, numClasses, categoricalFeaturesInfo,
      impurity, maxDepth, maxBins)

    // Evaluate model on test instances and compute test error
    val labelAndPreds = testData.map { point =>
      val prediction = model.predict(point.features)
      (BigDecimal(point.label).setScale(1, BigDecimal.RoundingMode.HALF_UP).toDouble,
        BigDecimal(prediction).setScale(1, BigDecimal.RoundingMode.HALF_UP).toDouble)
    }

    val avgErr = labelAndPreds.map(r => abs(r._1 - r._2)).sum() / testData.count()

    val correctCount = labelAndPreds.filter(row => row._1.equals(row._2)).count()

    val percentage: Float = correctCount.asInstanceOf[Float] / testData.count() * 100

    //println("Average Error = " + avgErr)
    println(s"Total: ${testData.count()}")
    println(s"Correct predictions: : $correctCount")

    println(s"Percentage: $percentage")


    //println("\nLearned classification tree model:\n" + model.toDebugString)


    //labelAndPreds foreach println


  }

}
