package FlightAnalysis

import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.DecisionTreeClassificationModel
import org.apache.spark.ml.classification.DecisionTreeClassifier
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.functions.udf

object FD_DT_ML_Classifier
{
  def main(args: Array[String]): Unit = {
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
      "PRCP", "SNOW", "SNWD", "TAVG", "TMAX", "TMIN", "WESF",
      "WT01", "WT02", "WT03", "WT04", "WT05", "WT06", "WT07", "WT08", "WT11"
    )

    //TODO: delay > 30min/40min
    def num2bolNum: (Float => Double) = v => { if (v > 20) 1.0 else 0.0 }

    val bool2int_udf = udf(num2bolNum)

    val convertLabel = finishedFrame
      .withColumn("label", bool2int_udf(dataFrame("DEP_DELAY")))


    //converting to RDD for mllib model
    val expandedFrame = convertLabel.rdd //.withColumn("SUM_DELAY", dataFrame("DEP_DELAY") + dataFrame("ARR_DELAY"))

    //ML DecisionTreeClassifier Variant
    // Automatically identify categorical features, and index them.
    // Here, we treat features with > 4 distinct values as continuous.
    val vectorAssembler = new VectorAssembler()
      .setInputCols(Array(
        "MONTH",
        "DAY_OF_MONTH",
        "DAY_OF_WEEK",
        "AIRLINE_ID",
        "ORIGIN_AIRPORT_ID",
        "DEST_AIRPORT_ID",
        "CRS_DEP_TIME",
        "DISTANCE_GROUP"
      ))
      .setOutputCol("features")
    //.setMaxCategories(4)
    //.fit(convertLabel)

    // Split the data into training and test sets (30% held out for testing).
    val Array(trainingData, testData) = convertLabel.randomSplit(Array(0.5, 0.5))

    // Train a DecisionTree model.
    val dt = new DecisionTreeClassifier()
      .setLabelCol("label")
      .setFeaturesCol("features")
      .setMaxDepth(3)
      .setMaxBins(50)

    // Chain indexer and tree in a Pipeline.
    val pipeline = new Pipeline().setStages(Array(vectorAssembler, dt))

    // Train model. This also runs the indexer.
    val model = pipeline.fit(trainingData)

    // Make predictions.
    val predictions = model.transform(testData)

    // Select example rows to display.
    predictions.select("prediction", "label", "features").cache()

    // Select (prediction, true label) and compute test error.
    val evaluator = new MulticlassClassificationEvaluator()
      .setLabelCol("label")
      .setPredictionCol("prediction")
      .setMetricName("accuracy")

    val accuracy = evaluator.evaluate(predictions)
    println("Test Error = " + (1.0 - accuracy))

    predictions.describe().show()

    predictions.show()

    val treeModel = model.stages(1).asInstanceOf[DecisionTreeClassificationModel]
    println("Learned classification tree model:\n" + treeModel.toDebugString)



  }

}
