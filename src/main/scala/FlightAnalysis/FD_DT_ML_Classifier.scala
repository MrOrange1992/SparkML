package FlightAnalysis

import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.{DecisionTreeClassificationModel, DecisionTreeClassifier, RandomForestClassificationModel, RandomForestClassifier}
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.{IndexToString, StringIndexer, VectorAssembler, VectorIndexer}
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.functions._


object FD_DT_ML_Classifier
{
  def main(args: Array[String]): Unit =
  {

    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    val flightDataFrameMapper: FlightDataMapper = new FlightDataMapper

    import flightDataFrameMapper.sparkSession.implicits._

    val flightDataFrame = flightDataFrameMapper.mappedFrameNoCancelled

    val californiaFrame = flightDataFrame.filter($"ORIGIN_STATE_ABR" === "CA")

    val flightFrame = californiaFrame.withColumnRenamed("FL_DATE", "DATE")

    val weatherFrame = flightDataFrameMapper.weatherFrame.filter($"name".contains("AIR"))

    val joinedFrame = flightFrame.join(weatherFrame, "DATE")

    def num2bolNum: (Float => Double) = v => { if (v > 10) 1.0 else 0.0 }

    val bool2int_udf = udf(num2bolNum)

    val dataFrame = joinedFrame.withColumn("IS_DELAYED", bool2int_udf(joinedFrame("DEP_DELAY"))).na.fill(0)


    val lrData = dataFrame
      .select(
        $"IS_DELAYED".as("label"),
        $"DAY_OF_MONTH",
        $"DAY_OF_WEEK",
        $"AIRLINE_ID",
        $"ORIGIN_AIRPORT_ID",
        $"DEST_AIRPORT_ID",
        $"CRS_DEP_TIME",
        $"DISTANCE_GROUP",
        $"PRCP",
        $"SNOW",
        $"SNWD",
        $"TAVG",
        $"TMAX",
        $"TMIN",
        $"WESF",
        $"WT01",
        $"WT02",
        $"WT03",
        $"WT04",
        $"WT05",
        $"WT06",
        $"WT07",
        $"WT08",
        $"WT11"
      )


    val featureArray = Array(
      "AIRLINE_ID",
      "ORIGIN_AIRPORT_ID",
      "DEST_AIRPORT_ID",
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


    //setting up features
    val assembler = new VectorAssembler().setInputCols(featureArray).setOutputCol("features")

    //mapped dataset for the decision tree classifier
    val dataDT = assembler.transform(lrData).select("label", "features")

    //splitting data into training data and test data
    val splitData = dataDT.randomSplit(Array(0.3, 0.7))
    val trainingData = splitData(0)
    val testData = splitData(1)

    //Decision tree classification model
    val classModel = new DecisionTreeClassifier()
    classModel.setMaxDepth(3)
    classModel.setMaxBins(200)
    //train the model
    val model = classModel.fit(trainingData)

    //compare feature importances
    val featureImportances = model.featureImportances
    val featureCoefficientMap = (featureArray zip featureImportances.toArray).map(entry => entry._1 -> entry._2).toMap
    val coefficientFrame = featureCoefficientMap.toSeq.toDF("name", "value").orderBy($"value".desc)

    println("Feature Importances:")
    coefficientFrame.show()

    //print model structure
    //println("Learned classification tree model:\n" + model.toDebugString)

    //compute predictions
    val predictions = model.transform(testData)

    //show residuals
    //predictions.select($"label", $"prediction").show()

    //compute metrics
    val allCount = predictions.count()
    val allLate = predictions.filter($"label" === 1).count()
    val correctPredictions = predictions.filter($"label" === $"prediction").count()
    val percentage = (correctPredictions.asInstanceOf[Float] / allCount) * 100

    println(s"Total flights: $allCount")
    println(s"Correct Predictions: $correctPredictions")
    println(s"Percentage: $percentage")





    /*
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    //instance for mapper class for sparkSession
    val dataFrameMapper: FlightDataMapper = new FlightDataMapper

    import dataFrameMapper.sparkSession.implicits._

    val dataFrame = dataFrameMapper.mappedFrameNoCancelled
    //val dataFrame = dataFrameMapper.sampleFlightFrame(1000)


    val newFrame = dataFrame.filter(dataFrame("ORIGIN_STATE_ABR") === "CA")
    val renamedFrame = newFrame.withColumnRenamed("FL_DATE", "DATE")
    //renamedFrame.describe().show()

    val weatherFrame = dataFrameMapper.weatherFrame.filter($"NAME".contains("AIR"))
    //weatherFrame.describe().show()

    /*val weatherFrame = dataFrameMapper.weatherFrame.filter(
      $"PRCP" > 0
        || $"WT01" === 1
        || $"WT02" === 1
        || $"WT03" === 1
        || $"WT04" === 1
        || $"WT05" === 1
        || $"WT06" === 1
        || $"WT07" === 1
        || $"WT08" === 1
        || $"WT11" === 1
    )//.filter($"NAME".contains("AIR"))
*/

    val completeFrame = renamedFrame.join(weatherFrame, "DATE").na.fill(0)

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
    )//.repartition(300).cache()

    def num2bolNum: (Float => Int) = v => { if (v > 25) 1 else 0 }

    val bool2int_udf = udf(num2bolNum)

    val convertLabel = finishedFrame
      .withColumn("label", bool2int_udf(dataFrame("DEP_DELAY"))).drop("DEP_DELAY").cache()

    //convertLabel.describe().show()

    // Index labels, adding metadata to the label column.
    // Fit on whole dataset to include all labels in index.
    // Necessary to avoid ClassCastException thrown during fitting data to pipeline
    val labelIndexer = new StringIndexer()
      .setInputCol("label")
      .setOutputCol("indexedLabel")
      .fit(convertLabel)


    val featureArray = Array(
      "MONTH",
      "DAY_OF_MONTH",
      "DAY_OF_WEEK",
      "AIRLINE_ID",
      "ORIGIN_AIRPORT_ID",
      "DEST_AIRPORT_ID",
      "CRS_DEP_TIME",
      "DISTANCE_GROUP",
      "PRCP", "SNOW", "SNWD", "TAVG", "TMAX", "TMIN", "WESF",
      "WT01", "WT02", "WT03", "WT04", "WT05", "WT06", "WT07", "WT08", "WT11"
    )

    val vectorAssembler = new VectorAssembler()
      .setInputCols(featureArray)
      .setOutputCol("features")

    // Split the data into training and test sets (30% held out for testing)
    val Array(trainingData, testData) = convertLabel.randomSplit(Array(0.7, 0.3))

    // Train a DecisionTree model.
    val dt = new DecisionTreeClassifier()
      .setLabelCol("indexedLabel")
      .setFeaturesCol("features")
      //.setMaxDepth(3)


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
    val totalCount = labelAndPreds.count()
    val correctCount = predictions.filter($"indexedLabel" === $"prediction").count()
    val percentage: Float = correctCount.asInstanceOf[Float] / labelAndPreds.count() * 100
    val totaldelayedFlights = predictions.filter(predictions("indexedLabel") === 1.0).count()
    val delayedFlightPercentage = totaldelayedFlights.asInstanceOf[Float] / totalCount * 100


    println(s"Total flights: $totalCount")
    println(s"Total delayed fligths percentage: $delayedFlightPercentage")
    println(s"Correct predictions: $correctCount")
    println(s"Percentage: $percentage")


    val treeModel = model.stages(2).asInstanceOf[DecisionTreeClassificationModel]

    val featureImportances = treeModel.featureImportances

    val featureCoefficientMap = (featureArray zip featureImportances.toArray).map(entry => entry._1 -> entry._2).toMap

    val coefficientFrame = featureCoefficientMap.toSeq.toDF("name", "value").orderBy($"value".desc)

    println("Feature Importances:")

    coefficientFrame.show()



    println(treeModel.featureImportances)


    //println("Learned classification tree model:\n" + treeModel.toDebugString)


    */

  }

}
