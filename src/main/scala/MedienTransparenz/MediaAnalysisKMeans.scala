package MedienTransparenz


import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.functions._



object MediaAnalysisKMeans
{
  def main(args: Array[String]): Unit =
  {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val dataFrameMapper: DataFrameMapper = new DataFrameMapper

    val dataFrame = dataFrameMapper.filterDF(true, true, false)

    //organisation key value map for string to numeric conversion
    val organisationColumn = dataFrame.select("organisation").groupBy("organisation").count().drop("count")
    val organisationMap = organisationColumn.rdd.zipWithIndex.map(row => (row._1(0).asInstanceOf[String], row._2)).collectAsMap()

    //media key value map for string to numeric conversion
    val mediaColumn = dataFrame.select("media").groupBy("media").count().drop("count")
    val mediaMap = mediaColumn.rdd.zipWithIndex.map(row => (row._1(0).asInstanceOf[String], row._2)).collectAsMap()

    //UDFs for assigning index key to org/media description
    def organisationToIndex: (String => Long) = v => organisationMap.getOrElse(v, -1)
    def mediaToIndex: (String => Long) = v => mediaMap.getOrElse(v, -1)
    //def federalStateToInt: (String => Int) = v => v.substring(3).toInt

    val organisationToIndex_udf = udf(organisationToIndex)
    val mediaToIndex_udf = udf(mediaToIndex)
    //val federalStateToInt_udf = udf(federalStateToInt)

    //setup of numeric frame for clustering algorithm
    val indexedFrame = dataFrame
      .withColumn("organisation", organisationToIndex_udf(dataFrame("organisation")))
      .withColumn("media", mediaToIndex_udf(dataFrame("media")))
      //.withColumn("federalState", federalStateToInt_udf(dataFrame("federalState")))

    //sum amount for media for organisation over all quarters
    val mediaSumInOrgDF = indexedFrame.groupBy("organisation", "media").sum("amount").withColumnRenamed("sum(amount)", "sumMediaByOrg")
    //mediaInOrgDF.orderBy(desc("sumMediaByOrg")).show()

    //total amounts for media
    val mediaSumTotalDF = indexedFrame.groupBy("media").sum("amount").withColumnRenamed("sum(amount)", "sumMediaTotal")
    //mediaTotal.orderBy(desc("sumMediaTotal")).show()

    //Table join to calculate percentage for media expenses
    val mediaJoinedDF = mediaSumInOrgDF.join(mediaSumTotalDF, "media")

    //new column with calculated percentage of expenses from org for media
    val mediaPctInOrgDF = mediaJoinedDF
        .withColumn("%", (mediaJoinedDF("sumMediaByOrg") / mediaJoinedDF("sumMediaTotal")) * 100)

    //mediaPctInOrgDF.show()

    //create pivot table for media columns and organisation rows with percentage value cells
    val pivotTable = mediaPctInOrgDF
      .groupBy("organisation")
      .pivot("media")                             //pivot element for aggregate function
      .agg(sum(mediaPctInOrgDF("%")))             //aggregate with percentage of spendings by org
      .sort("organisation")
      .na.fill(0)                                 //replace null values with 0

    pivotTable.show()



    /*
    //setting up features
    val assembler = new VectorAssembler()
      .setInputCols(Array(
        "media",
        "amount"
      )).setOutputCol("features")

    //mapped dataset
    val dataFrameKM = assembler.transform(indexedFrame).select("features").cache()

    //dataFrameKM.show()


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
    */


  }

}


// OLD CODE

//sanity check organisation
//println(indexedFrame.select().where("organisation='678'").count())
//println(organisationMap.map(entry => entry._2 -> entry._1).get(678)) //returns 712
//712 matches in csv search for "Agrarmarkt Austria Marketing GesmbH"

//val reducedMedia: Array[String] = indexedFrame.select("media").groupBy("media").count().drop("count").rdd.map(row => row(0).toString).collect()

//reducedMedia foreach println

//indexedFrame.rdd.map(row => row(2)) foreach println

//df.withColumn("prodID", explode(col("prodIDList")))

//val mediaAmountMap = indexedFrame.groupBy("media").sum("amount").rdd.map(row => row(0).toString -> row(1).toString).collect().toMap //.rdd.map((a, b) => (a(0), b(0))) foreach println//.map(row => (a, b) => )//.withColumn("media", explode(indexedFrame("amount"))).show()

//mediaAmountMap foreach println

//val mediaFrame = indexedFrame.groupBy("media").sum("amount")

//mediaFrame.show()

/*
  Sum spendings of each organisation to certain media
 */

/*

    val percentageTable = pivotTable
      .columns
      .foldLeft(pivotTable) { (memoDF, colName) =>
        memoDF
          .withColumn(
            colName,
            col(colName) / sum(col(colName)).over()
            //colName.toLowerCase().replace(" ", "_")
          )
      }

    percentageTable.show()

    */

/*
  .groupBy("code")
  .agg(sum("count").alias("count"))
  .withColumn("fraction", col("count") /  sum("count").over())
  */

//mediaJoinedDF.sort("media").show()

//val clusteringMap: scala.collection.mutable.Map[Int, ((Int, Float))] = scala.collection.mutable.Map()


/*


val clusteringMap = indexedFrame.rdd.map(row => {
  //clusteringMap(row(1).toString.toInt, (row(0).toString.toInt, row(3).toString.toFloat))
  row(0).toString.toInt -> (row(2).toString.toInt, row(5).toString.toFloat)
}).groupBy(_._1).mapValues(_.map(_._2).toMap).collectAsMap()
//m.groupBy(x => x._2).mapValues(_.keys.toList)

//println(clusteringMap.keys.count())






val clusterArray: Array[Array[Float]] = Array.ofDim[Float](clusteringMap.keys.size, 4185)

for (org <- clusteringMap.keys) {

  for (med <- 0 to 4184) {
    /*clusterArray(org)(med) = clusteringMap.get(org).collect {
      case (med, b) => b
      case _ => 0f
    }.*/

    //map.get('type).map("prefix" + _).getOrElse("")

    clusterArray(org)(med) = clusteringMap.get(org).get.get(med).getOrElse(0)

    //clusterArray(org)(med) = clusteringMap.get(org).filter(_._1.toInt.equals(med.toInt)).map(_._2).getOrElse(0)


  }
}

*/
//clusterArray.foreach(_.foreach(println(_)))

//val df = dataFrameMapper.sparkSession.sparkContext.parallelize(clusteringMap.toArray)

//df foreach println

//clusterArray.map()


//clusteringMap.values.reduce((a ,b) => if (a._1 == b._1) (a._1, a._2 + b._2) else a)

//println(clusteringMap.keys.size)



//indexedFrame.groupBy("media").count().describe().show()


/*
import dataFrameMapper.sparkSession.implicits._

val df = mediaAmountMap.toSeq.toDF("mediaIndex", "amount")

df.show()
*/

//val dataFr = (one(0), two(0))._1

//indexedFrame.show()