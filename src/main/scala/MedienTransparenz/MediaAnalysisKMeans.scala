package MedienTransparenz


import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.functions._



object MediaAnalysisKMeans
{
  def main(args: Array[String]): Unit =
  {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val dataFrameMapper: DataFrameMapper = new DataFrameMapper

    val dataFrame = dataFrameMapper.filterDF(true, true, false)

    val filteredFrame = dataFrame
      .filter(dataFrame("organisation").contains("ministerium") || dataFrame("organisation").contains("kanzleramt"))
      .filter(dataFrame("period") >= 20141)   //start for Faymann
      .filter(dataFrame("period") =!= 20162) //unsure of influence -> 17.05.2016 Faymann handing over to Kern

    //filteredFrame.describe().show()



    val nonPoliticalFrame = dataFrame
      .filter(!dataFrame("organisation").contains("ministerium") || !dataFrame("organisation").contains("kanzleramt"))
      .filter(dataFrame("period") >= 20141)   //start for Faymann
      .filter(dataFrame("period") =!= 20162) //unsure of influence -> 17.05.2016 Faymann handing over to Kern



    //val nonPolSumByMedia = nonPolTaggedFrame.groupBy("media").sum("amount")

    //nonPoliticalFrame.describe().show()

    /*
    //organisation key value map for string to numeric conversion
    val organisationColumn = filteredFrame.select("organisation").groupBy("organisation").count().drop("count")
    val organisationMap = organisationColumn.rdd.zipWithIndex.map(row => (row._1(0).asInstanceOf[String], row._2)).collectAsMap()

    //media key value map for string to numeric conversion
    val mediaColumn = filteredFrame.select("media").groupBy("media").count().drop("count")
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

      */

    //sum amount for media for organisation over all quarters
    //val orgSumAmount = indexedFrame.groupBy("organisation", "media").sum("amount").withColumnRenamed("sum(amount)", "sumMediaByOrg")
    val mediaSumAmountByMedia = filteredFrame.groupBy("media", "organisation").sum("amount").withColumnRenamed("sum(amount)", "sumAmountByOrg")

    val mediaSumAmountByMediaNonPol= nonPoliticalFrame.groupBy("media", "organisation").sum("amount").withColumnRenamed("sum(amount)", "sumAmountByOrgNonPol")


    //mediaSumAmountByMedia.describe().show()
    //mediaSumAmountByMedia.orderBy("media").show()



    //mediaInOrgDF.orderBy(desc("sumMediaByOrg")).show()

    //total amounts for media
    val sumAmountTotalByOrg = filteredFrame.groupBy("organisation").sum("amount").withColumnRenamed("sum(amount)", "sumOrgTotal")
    val sumAmountTotalByOrgNonPol = nonPoliticalFrame.groupBy("organisation").sum("amount").withColumnRenamed("sum(amount)", "sumOrgTotalNonPol")

    //mediaTotal.orderBy(desc("sumMediaTotal")).show()



    //Table join to calculate percentage for media expenses
    val orgJoinedDF = mediaSumAmountByMedia.join(sumAmountTotalByOrg, "organisation")
    val orgJoinedDFNonPol = mediaSumAmountByMediaNonPol.join(sumAmountTotalByOrgNonPol, "organisation")


    //new column with calculated percentage of expenses from org for media
    val orgPctForMedia = orgJoinedDF
    .withColumn("%", (orgJoinedDF("sumAmountByOrg") / orgJoinedDF("sumOrgTotal")) * 100)

    val orgPctForMediaNonPol = orgJoinedDFNonPol
      .withColumn("%", (orgJoinedDFNonPol("sumAmountByOrgNonPol") / orgJoinedDFNonPol("sumOrgTotalNonPol")) * 100)
    //orgPctForMedia.sort(desc("%")).show()



    //val expandedFrame = filteredFrame.with

    /*
       Unknown -> 0
       ÖVP     -> 1
       SPÖ     -> 2
       Both    -> 3
       Other   -> 99
     */


    def orgToPolOrient = udf((orgName: String) => {
      if (orgName == "Bundeskanzleramt") 3
      else if (orgName.contains("Arbeit")) 2
      else if (orgName.contains("Bildung")) 2
      else if (orgName.contains("Europa")) 1
      else if (orgName.contains("Familie")) 9
      else if (orgName.contains("Finanzen")) 1
      else if (orgName.contains("Gesundheit")) 2
      else if (orgName.contains("Inneres")) 1
      else if (orgName.contains("Umwelt")) 1
      else if (orgName.contains("Landesverteidigung")) 2
      else if (orgName.contains("Verkehr")) 2
      else if (orgName.contains("Wissenschaft")) 1
      else 999
    })



    val polOrientFrame = orgPctForMedia.withColumn("polOr", orgToPolOrient(orgPctForMedia("organisation")))


    def orgToNonPolitical = udf((orgName: String) => 0 )

    val nonPolTaggedFrame = orgPctForMediaNonPol.withColumn("polOr", orgToNonPolitical(orgPctForMediaNonPol("organisation")))


    //polOrientFrame.show()





    val mediaPolOrientedMap = polOrientFrame.groupBy("media").sum().rdd.map(row => row.get(0).toString).collect().toSet


    //nonPolSumByMedia.show()



    //polOrientFrame.show(false)

    //polOrientFrame.groupBy("media").sum().describe().show()


    /*

    //create pivot table for media columns and organisation rows with percentage value cells
    val pivotTable = mediaPctInOrgDF
      .groupBy("organisation")
      .pivot("media")                             //pivot element for aggregate function
      .agg(sum(mediaPctInOrgDF("%")))             //aggregate with percentage of spendings by org
      .sort("organisation")
      .na.fill(0)                                 //replace null values with 0

    //pivotTable.describe().show()


    pivotTable.show()







    //setting up features
    val assembler = new VectorAssembler()
      .setInputCols(
        (0 to pivotTable.count().toInt).map(number => number.toString).toArray
      ).setOutputCol("features")

    //mapped dataset
    val dataFrameKM = assembler.transform(pivotTable).select("features").cache()

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

    dataFrameMapper.sparkSession.stop()


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



  /*

def orgToPolOrient = udf((date: Int, orgName: String) =>
{
if (dateToChancellorMap.getOrElse(date, "") == "Faymann")
{
if (orgName == "Bundeskanzleramt") 3
else if (orgName.contains("Arbeit")) 2
else if (orgName.contains("Bildung")) 2
else if (orgName.contains("Europa")) 1
else if (orgName.contains("Familie")) 9
else if (orgName.contains("Finanzen")) 1
else if (orgName.contains("Gesundheit")) 2
else if (orgName.contains("Inneres")) 1
else if (orgName.contains("Umwelt")) 1
else if (orgName.contains("Landesverteidigung")) 2
else if (orgName.contains("Verkehr")) 2
else if (orgName.contains("Wissenschaft")) 1
else 999
}
else if (dateToChancellorMap.getOrElse(date, "") == "Kern")
{
if (orgName == "Bundeskanzleramt") 3
else if (orgName.contains("Arbeit")) 2
else if (orgName.contains("Bildung")) 2
else if (orgName.contains("Europa")) 1
else if (orgName.contains("Familie")) 9
else if (orgName.contains("Finanzen")) 1
else if (orgName.contains("Gesundheit")) 2
else if (orgName.contains("Inneres")) 1
else if (orgName.contains("Umwelt")) 1
else if (orgName.contains("Landesverteidigung")) 2
else if (orgName.contains("Verkehr")) 2
else if (orgName.contains("Wissenschaft")) 1
else 999
}
else 999
})



val dateToChancellorMap: Map[Int, String] = Map(
      20141 -> "Faymann",
      20142 -> "Faymann",
      20143 -> "Faymann",
      20144 -> "Faymann",
      20151 -> "Faymann",
      20152 -> "Faymann",
      20153 -> "Faymann",
      20154 -> "Faymann",
      20161 -> "Faymann",
      20163 -> "Kern",
      20164 -> "Kern",
      20171 -> "Kern",
      20172 -> "Kern"
    )
  */