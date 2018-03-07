package MedienTransparenz

import co.theasi.plotly.{MarkerOptions, Plot, ScatterMode, ScatterOptions, draw, writer}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.clustering.{KMeans, KMeansModel}
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{sum, udf}

import scala.util.Random

object KMeansBookExample
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
       Other   -> 9
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




    //organisation key value map for string to numeric conversion
    val organisationColumn = polOrientFrame.select("organisation").groupBy("organisation").count().drop("count")
    val organisationMap = organisationColumn.rdd.zipWithIndex.map(row => (row._1(0).asInstanceOf[String], row._2)).collectAsMap()

    //media key value map for string to numeric conversion
    val mediaColumn = polOrientFrame.select("media").groupBy("media").count().drop("count")
    val mediaMap = mediaColumn.rdd.zipWithIndex.map(row => (row._1(0).asInstanceOf[String], row._2)).collectAsMap()

    //UDFs for assigning index key to org/media description
    def organisationToIndex: (String => Long) = v => organisationMap.getOrElse(v, -1)
    def mediaToIndex: (String => Long) = v => mediaMap.getOrElse(v, -1)
    //def federalStateToInt: (String => Int) = v => v.substring(3).toInt

    val organisationToIndex_udf = udf(organisationToIndex)
    val mediaToIndex_udf = udf(mediaToIndex)
    //val federalStateToInt_udf = udf(federalStateToInt)

    //setup of numeric frame for clustering algorithm
    val indexedFrame = polOrientFrame
      .withColumn("organisation", organisationToIndex_udf(dataFrame("organisation")))
      .withColumn("media", mediaToIndex_udf(dataFrame("media")))
      //.withColumn("federalState", federalStateToInt_udf(dataFrame("federalState")))

    //indexedFrame.describe().show()



    //nonPolSumByMedia.show()



    //polOrientFrame.show(false)

    //polOrientFrame.groupBy("media").sum().describe().show()



/*
    //create pivot table for media columns and organisation rows with percentage value cells
    val pivotTable = indexedFrame
      .groupBy("organisation")
      .pivot("media")                             //pivot element for aggregate function
      .agg(sum(indexedFrame("%")))             //aggregate with percentage of spendings by org
      .sort("organisation")
      .na.fill(0)                                 //replace null values with 0

    //pivotTable.describe().show()


    pivotTable.show()
    */

    val numericFrame = indexedFrame.filter(_ != "sumAmountByOrg").filter(_ != "sumOrgTotal")


    numericFrame.describe().show()


    val assembler = new VectorAssembler().setInputCols(numericFrame.columns).setOutputCol("features")

    /*
    //setting up features
    val assembler = new VectorAssembler()
      .setInputCols(
        (0 to pivotTable.count().toInt).map(number => number.toString).toArray
      ).setOutputCol("features")
      */


    val kmeans = new KMeans().setPredictionCol("cluster").setFeaturesCol("features")

    val pipeline = new Pipeline().setStages(Array(assembler, kmeans))

    val pipelineModel = pipeline.fit(indexedFrame)

    val kMeansModel = pipelineModel.stages.last.asInstanceOf[KMeansModel]

    kMeansModel.clusterCenters foreach println


    val withCluster = pipelineModel.transform(numericFrame)

    val wCluster = withCluster.select("cluster", "polOr").groupBy("cluster", "polOr").count()

    wCluster.orderBy(wCluster("cluster"), wCluster("count").desc).show()




    def clusteringScore(data: DataFrame, k: Int): Double =
    {
      val assembler = new VectorAssembler().setInputCols(data.columns).setOutputCol("features")

      val kMeans = new KMeans()
        .setSeed(Random.nextLong())
        .setK(k)
          .setMaxIter(40)
          .setTol(1.0e-5)
        .setPredictionCol("cluster")
        .setFeaturesCol("features")

      val pipeline = new Pipeline().setStages(Array(assembler, kMeans))

      val kMeansModel = pipeline.fit(data).stages.last.asInstanceOf[KMeansModel]

      kMeansModel.computeCost(assembler.transform(data)) / data.count()
    }

    val kList = (2 to 16 by 2 ).map(k => clusteringScore(numericFrame, k))




    //PLOTLY
    //------------------------------------------------------------------------------------------------------------------


    implicit val x: Array[Double] = (2.asInstanceOf[Double] to 16 by 2).toArray
    implicit val y: Array[Double] = kList.toArray




    // Options common to traces
    val commonOptions = ScatterOptions().mode(ScatterMode.Marker).marker(MarkerOptions().size(8).lineWidth(1))


    // The plot itself
    val plot = Plot()
      .withScatter(x, y, commonOptions.name("K"))



    draw(plot, "MT_KMeans", writer.FileOptions(overwrite=true))


    //stop session


    dataFrameMapper.sparkSession.stop()


  }


}
