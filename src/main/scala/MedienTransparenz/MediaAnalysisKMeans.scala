package MedienTransparenz

import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.types.{DoubleType, IntegerType}



object MediaAnalysisKMeans
{
  def main(args: Array[String]): Unit =
  {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val dataFrameMapper: DataFrameMapper = new DataFrameMapper

    val dataFrame = dataFrameMapper.filterDF(true, true, false)

    val organisationColumn = dataFrame.select("organisation").groupBy("organisation").count().drop("count")
    val organisationMap = organisationColumn.rdd.zipWithIndex.map(row => (row._1(0).asInstanceOf[String], row._2)).collectAsMap()

    val mediaColumn = dataFrame.select("media").groupBy("media").count().drop("count")
    val mediaMap = mediaColumn.rdd.zipWithIndex.map(row => (row._1(0).asInstanceOf[String], row._2)).collectAsMap()

    def organisationToIndex: (String => Long) = v => organisationMap.getOrElse(v, -1)
    def mediaToIndex: (String => Long) = v => mediaMap.getOrElse(v, -1)
    //def federalStateToInt: (String => Int) = v => v.substring(3).toInt


    val organisationToIndex_udf = udf(organisationToIndex)
    val mediaToIndex_udf = udf(mediaToIndex)
    //val federalStateToInt_udf = udf(federalStateToInt)

    val indexedFrame = dataFrame
      .withColumn("organisation", organisationToIndex_udf(dataFrame("organisation")))
      .withColumn("media", mediaToIndex_udf(dataFrame("media")))
      //.withColumn("federalState", federalStateToInt_udf(dataFrame("federalState")))
      //.show()



    //sanity check organisation
    //println(indexedFrame.select().where("organisation='678'").count())
    //println(organisationMap.map(entry => entry._2 -> entry._1).get(678)) //returns 712
    //712 matches in csv search for "Agrarmarkt Austria Marketing GesmbH"

    val reducedMedia: Array[String] = indexedFrame.select("media").groupBy("media").count().drop("count").rdd.map(row => row(0).toString).collect()

    reducedMedia foreach println


    //setting up features
    val assembler = new VectorAssembler()
      .setInputCols(
        /*
        //"federalState",
        "transferType",
        "period",
        "amount"*/
        reducedMedia
      ).setOutputCol("features")

    //mapped dataset
    val dataFrameKM = assembler.transform(indexedFrame).select("amount", "features").cache()

    //dataFrameKM.show()


    /*
    // Crates a DataFrame
    val dataset: DataFrame = sparkSession.sqlContext.createDataFrame(Seq(
      (1, Vectors.dense(0.0, 0.0, 0.0)),
      (2, Vectors.dense(0.1, 0.1, 0.1)),
      (3, Vectors.dense(0.2, 0.2, 0.2)),
      (4, Vectors.dense(9.0, 9.0, 9.0)),
      (5, Vectors.dense(9.1, 9.1, 9.1)),
      (6, Vectors.dense(9.2, 9.2, 9.2))
    )).toDF("id", "features")
    */


    // Trains a k-means model
    val kmeans = new KMeans()
      .setK(5)
      .setFeaturesCol("features")
      .setPredictionCol("prediction")

    val model = kmeans.fit(dataFrameKM)

    val WSSSE = model.computeCost(dataFrameKM)
    println(s"Within Set Sum of Squared Errors = $WSSSE")



    // Shows the result
    println("Final Centers: ")
    model.clusterCenters.foreach(println)


  }

}
