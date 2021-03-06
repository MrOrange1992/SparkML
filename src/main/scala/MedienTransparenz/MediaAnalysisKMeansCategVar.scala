package MedienTransparenz

import co.theasi.plotly.{draw, MarkerOptions, Plot, ScatterMode, ScatterOptions, writer}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.clustering.{KMeans, KMeansModel}
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.udf
import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.sql.SQLImplicits

import scala.util.Random

object MediaAnalysisKMeansCategVar
{
  def main(args: Array[String]): Unit = {


    //setting up spark and data
    //------------------------------------------------------------------------------------------------------------------
    Logger.getLogger("org").setLevel(Level.ERROR)
    val dataFrameMapper: DataFrameMapper = new DataFrameMapper
    import dataFrameMapper.sparkSession.sqlContext.implicits._

    val dataFrame = dataFrameMapper.filterDF(true, true, false)

    val filteredFrame = dataFrame
      .filter(dataFrame("organisation").contains("ministerium") || dataFrame("organisation").contains("kanzleramt"))
      .filter(dataFrame("period") >= 20141) //start for Faymann
      .filter(dataFrame("period") =!= 20162) //unsure of influence -> 17.05.2016 Faymann handing over to Kern


    val nonPoliticalFrame = dataFrame
      .filter(!dataFrame("organisation").contains("ministerium") || !dataFrame("organisation").contains("kanzleramt"))
      .filter(dataFrame("period") >= 20141) //start for Faymann
      .filter(dataFrame("period") =!= 20162) //unsure of influence -> 17.05.2016 Faymann handing over to Kern


    //sum amount for media for organisation over all quarters
    //val orgSumAmount = indexedFrame.groupBy("organisation", "media").sum("amount").withColumnRenamed("sum(amount)", "sumMediaByOrg")
    val mediaSumAmountByMedia = filteredFrame.groupBy("media", "organisation").sum("amount").withColumnRenamed("sum(amount)", "sumAmountByOrg")
    val mediaSumAmountByMediaNonPol = nonPoliticalFrame.groupBy("media", "organisation").sum("amount").withColumnRenamed("sum(amount)", "sumAmountByOrgNonPol")


    //total amounts for media
    val sumAmountTotalByOrg = filteredFrame.groupBy("organisation").sum("amount").withColumnRenamed("sum(amount)", "sumOrgTotal")
    val sumAmountTotalByOrgNonPol = nonPoliticalFrame.groupBy("organisation").sum("amount").withColumnRenamed("sum(amount)", "sumOrgTotalNonPol")


    //Table join to calculate percentage for media expenses
    val orgJoinedDF = mediaSumAmountByMedia.join(sumAmountTotalByOrg, "organisation")
    val orgJoinedDFNonPol = mediaSumAmountByMediaNonPol.join(sumAmountTotalByOrgNonPol, "organisation")


    //new column with calculated percentage of expenses from org for media
    val orgPctForMedia = orgJoinedDF
      .withColumn("%", (orgJoinedDF("sumAmountByOrg") / orgJoinedDF("sumOrgTotal")) * 100)

    val orgPctForMediaNonPol = orgJoinedDFNonPol
      .withColumn("%", (orgJoinedDFNonPol("sumAmountByOrgNonPol") / orgJoinedDFNonPol("sumOrgTotalNonPol")) * 100)
    //------------------------------------------------------------------------------------------------------------------


    /*
      create new column with political orientation
      based on organisation as numeric values for KMeans model
     */
    //------------------------------------------------------------------------------------------------------------------
    /*
       Unknown -> 0
       ÖVP     -> 1
       SPÖ     -> 2
       Both    -> 3
       Other   -> 9
     */

    def orgToPolOrient(columnName: String) = udf((orgName: String) => {
      if (columnName == "isOVP") {
        if (orgName == "Bundeskanzleramt") 1
        else if (orgName.contains("Arbeit")) 0
        else if (orgName.contains("Bildung")) 0
        else if (orgName.contains("Europa")) 1
        else if (orgName.contains("Familie")) 0
        else if (orgName.contains("Finanzen")) 1
        else if (orgName.contains("Gesundheit")) 0
        else if (orgName.contains("Inneres")) 1
        else if (orgName.contains("Umwelt")) 1
        else if (orgName.contains("Landesverteidigung")) 0
        else if (orgName.contains("Verkehr")) 0
        else if (orgName.contains("Wissenschaft")) 1
        else 0 //dummy for other if filter went wrong
      }
      else if (columnName == "isSPO") {
        if (orgName == "Bundeskanzleramt") 1
        else if (orgName.contains("Arbeit")) 1
        else if (orgName.contains("Bildung")) 1
        else if (orgName.contains("Europa")) 0
        else if (orgName.contains("Familie")) 0
        else if (orgName.contains("Finanzen")) 0
        else if (orgName.contains("Gesundheit")) 1
        else if (orgName.contains("Inneres")) 0
        else if (orgName.contains("Umwelt")) 0
        else if (orgName.contains("Landesverteidigung")) 1
        else if (orgName.contains("Verkehr")) 1
        else if (orgName.contains("Wissenschaft")) 0
        else 0 //dummy for other if filter went wrong
      }
      else if (columnName == "isOther") {
        if (orgName == "Bundeskanzleramt") 0
        else if (orgName.contains("Arbeit")) 0
        else if (orgName.contains("Bildung")) 0
        else if (orgName.contains("Europa")) 0
        else if (orgName.contains("Familie")) 1
        else if (orgName.contains("Finanzen")) 0
        else if (orgName.contains("Gesundheit")) 0
        else if (orgName.contains("Inneres")) 0
        else if (orgName.contains("Umwelt")) 0
        else if (orgName.contains("Landesverteidigung")) 0
        else if (orgName.contains("Verkehr")) 0
        else if (orgName.contains("Wissenschaft")) 0
        else 0 //dummy for other if filter went wrong
      }

      else 0
    })

    val list = List("isOVP", "isSPO", "isOther")

    val newFrame = list.foldLeft(orgPctForMedia)((df, bool) => df.withColumn(bool, orgToPolOrient(bool)(df("organisation"))))



    //------------------------------------------------------------------------------------------------------------------


    /*
        String to numeric conversion of orgNames / mediaNames
        for KMeans model
     */
    //------------------------------------------------------------------------------------------------------------------
    //organisation key value map for string to numeric conversion
    val organisationColumn = newFrame.select("organisation").groupBy("organisation").count().drop("count")
    val organisationMap = organisationColumn.rdd.zipWithIndex.map(row => (row._1(0).asInstanceOf[String], row._2)).collectAsMap()

    //media key value map for string to numeric conversion
    val mediaColumn = newFrame.select("media").groupBy("media").count().drop("count")
    val mediaMap = mediaColumn.rdd.zipWithIndex.map(row => (row._1(0).asInstanceOf[String], row._2)).collectAsMap()

    //UDFs for assigning index key to org/media description
    def organisationToIndex: (String => Long) = v => organisationMap.getOrElse(v, -1)

    def mediaToIndex: (String => Long) = v => mediaMap.getOrElse(v, -1)
    //def federalStateToInt: (String => Int) = v => v.substring(3).toInt

    val organisationToIndex_udf = udf(organisationToIndex)
    val mediaToIndex_udf = udf(mediaToIndex)
    //val federalStateToInt_udf = udf(federalStateToInt)

    //setup of numeric frame for clustering algorithm
    val indexedFrame = newFrame
      .withColumn("organisation", organisationToIndex_udf(dataFrame("organisation")))
      .withColumn("media", mediaToIndex_udf(dataFrame("media")))
    //------------------------------------------------------------------------------------------------------------------


    //create KMeans model
    //------------------------------------------------------------------------------------------------------------------
    def computeCost(data: DataFrame, k: Int): Double = {
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


    def computeKMeans(data: DataFrame, k: Int): KMeans = {
      new KMeans().setSeed(Random.nextLong()).setK(k).setMaxIter(40).setTol(1.0e-5)
        .setPredictionCol("cluster").setFeaturesCol("features")
    }


    val numericFrame = indexedFrame.filter(_ != "sumAmountByOrg").filter(_ != "sumOrgTotal")
    numericFrame.describe().show()


    val assembler = new VectorAssembler().setInputCols(numericFrame.columns).setOutputCol("features")

    val kmeans = computeKMeans(numericFrame, 8)

    val pipeline = new Pipeline().setStages(Array(assembler, kmeans))
    val pipelineModel = pipeline.fit(indexedFrame)

    val kMeansModel = pipelineModel.stages.last.asInstanceOf[KMeansModel]


    val withCluster = pipelineModel.transform(numericFrame)

    val wCluster = withCluster.select("cluster", "isOVP", "isSPO", "isOther").groupBy("cluster", "isOVP", "isSPO", "isOther").count()

    wCluster.orderBy(wCluster("cluster"), wCluster("count").desc).show()






    //------------------------------------------------------------------------------------------------------------------






    //PLOTLY          ELBOW PLOT for getting best k
    //------------------------------------------------------------------------------------------------------------------

    /*
    // best k seems to be 6 -> plotly elbow plot
    val kList = (2 to 16 by 2 ).map(k => computeCost(numericFrame, k))

    implicit val x: Array[Double] = (2.asInstanceOf[Double] to 16 by 2).toArray
    implicit val y: Array[Double] = kList.toArray

    // Options common to traces
    val commonOptions = ScatterOptions().mode(ScatterMode.Marker).marker(MarkerOptions().size(8).lineWidth(1))

    // The plot itself
    val plot = Plot().withScatter(x, y, commonOptions.name("K"))

    draw(plot, "MT_KMeans_CategVar", writer.FileOptions(overwrite=true))

    */
    //------------------------------------------------------------------------------------------------------------------



    dataFrameMapper.sparkSession.stop()
  }
}
