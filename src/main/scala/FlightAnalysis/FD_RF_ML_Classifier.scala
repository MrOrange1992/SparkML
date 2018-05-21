package FlightAnalysis

import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.{DecisionTreeClassificationModel, DecisionTreeClassifier, RandomForestClassificationModel, RandomForestClassifier}
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.{IndexToString, StringIndexer, VectorAssembler}
import org.apache.spark.sql.functions.udf

object FD_RF_ML_Classifier
{
  def main(args: Array[String]): Unit = {
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    //instance for mapper class for sparkSession
    val dataFrameMapper: FlightDataMapper = new FlightDataMapper

    import dataFrameMapper.sparkSession.implicits._

    val dataFrame = dataFrameMapper.mappedFrameNoCancelled
    //val dataFrame = dataFrameMapper.sampleFlightFrame(1000)


    val newFrame = dataFrame//.filter(dataFrame("ORIGIN_STATE_ABR") === "CA")

    //val renamedFrame = newFrame.withColumnRenamed("FL_DATE", "DATE")
    //renamedFrame.describe().show()

    //val weatherFrame = dataFrameMapper.weatherFrame
    //val weatherFrame = dataFrameMapper.sampleWeatherFrame(1000)
    //weatherFrame.describe().show()

    //val completeFrame = renamedFrame.join(weatherFrame, "DATE")
    //completeFrame.describe().show()

    val finishedFrame = newFrame.select(
      "MONTH",
      "DAY_OF_MONTH",
      "DAY_OF_WEEK",
      "AIRLINE_ID",
      "ORIGIN_AIRPORT_ID",
      "DEST_AIRPORT_ID",
      "CRS_DEP_TIME",
      "DEP_DELAY",
      "DISTANCE_GROUP"
      //"PRCP", "SNOW", "SNWD", "TAVG", "TMAX", "TMIN", "WESF"
      //"WT01", "WT02", "WT03", "WT04", "WT05", "WT06", "WT07", "WT08", "WT11"
    )

    def num2bolNum: (Float => Int) = v => { if (v > 25) 1 else 0 }

    val bool2int_udf = udf(num2bolNum)

    val convertLabel = finishedFrame
      .withColumn("label", bool2int_udf(dataFrame("DEP_DELAY"))).drop("DEP_DELAY")

    //convertLabel.describe().show()

    // Index labels, adding metadata to the label column.
    // Fit on whole dataset to include all labels in index.
    // Necessary to avoid ClassCastException thrown during fitting data to pipeline
    val labelIndexer = new StringIndexer()
      .setInputCol("label")
      .setOutputCol("indexedLabel")
      .fit(convertLabel)

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
        //"PRCP", "SNOW", "SNWD", "TAVG", "TMAX", "TMIN", "WESF"
        //"WT01", "WT02", "WT03", "WT04", "WT05", "WT06", "WT07", "WT08", "WT11"
      ))
      .setOutputCol("features")

    // Split the data into training and test sets (30% held out for testing)
    val Array(trainingData, testData) = convertLabel.randomSplit(Array(0.7, 0.3))

    // Train a DecisionTree model.
    val dt = new RandomForestClassifier()
      .setLabelCol("indexedLabel")
      .setFeaturesCol("features")

    // Convert indexed labels back to original labels.
    val labelConverter = new IndexToString()
      .setInputCol("prediction")
      .setOutputCol("predictedLabel")
      .setLabels(labelIndexer.labels)

    // Chain indexers and tree in a Pipeline
    val pipeline = new Pipeline().setStages(Array(labelIndexer, vectorAssembler, dt, labelConverter))

    // Train model.  This also runs the indexers.
    val model = pipeline.fit(trainingData)


    // Make predictions.
    val predictions = model.transform(testData)

    // Select (prediction, true label) and compute test error
    val evaluator = new MulticlassClassificationEvaluator()
      .setLabelCol("indexedLabel")
      .setPredictionCol("prediction")
      .setMetricName("accuracy")

    val accuracy = evaluator.evaluate(predictions)
    println("Test Error = " + (1.0 - accuracy))


    //get predictions from testData
    val labelAndPreds = predictions.select("indexedLabel", "prediction")
    val correctCount = predictions.filter($"indexedLabel" === $"prediction").count()
    val percentage: Float = correctCount / labelAndPreds.count() * 100

    val totaldelayedFlights = predictions.filter(predictions("indexedLabel") === 1.0).count()



    println(s"\nTotal flights: ${labelAndPreds.count()}")
    println(s"Total delayed fligths: $totaldelayedFlights")

    println(s"\nCorrect predictions: $correctCount")
    println(s"Percentage: $percentage")


    val treeModel = model.stages(2).asInstanceOf[RandomForestClassificationModel]

    println(treeModel.featureImportances)
    println("Learned classification tree model:\n" + treeModel.toDebugString)

  }

}
