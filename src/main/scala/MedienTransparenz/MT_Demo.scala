package MedienTransparenz

import org.apache.log4j.{Level, Logger}

object MT_Demo
{
  def main(args: Array[String]): Unit =
  {
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    //instance for mapper class for sparkSession
    val dataFrameMapper: DataFrameMapper = new DataFrameMapper

    val dataFrame = dataFrameMapper.filterDF("31")
    //dataFrame.show()

    //show count for transferTypes in data
    //dataFrame.groupBy("transferType").count().show()
    //dataFrame.groupBy("transferType").sum("amount").show()

    //sum amount for media for organisation over all quarters
    val mediaInOrgDF = dataFrame.groupBy("organisation", "media").sum("amount")
    mediaInOrgDF.show()
    /*
        sanitation checks:
          mediaInOrgDF.describe().show()
          mediaInOrgDF.filter(mediaInOrgDF("organisation") === "HYPO NOE Gruppe Bank AG").show(50)

          evaluated sum of media for org in .csv -> checks out
     */







    // RDD variants
    // -------------------------------------------------------------------------------------
    //type mediaRTMap = Map[String, List[(String, Float)]]

    //val mediaRDD =  dataFrame.rdd

    //val mediaMap = mediaRDD.map(r => (r(0), List((r(4), r(5))))).reduceByKey((a, b) => if (a.head._1 == b.head._1) {println(a.head._1); List((a.head._1, a.head._2.asInstanceOf[Float] + b.head._2.asInstanceOf[Float]))} else a ++ b)
    //val mediaMap = mediaRDD.map(r => (r(0), List((r(4), r(5))))).reduceByKey((a, b) => a ++ b)//.map(v => v._1 -> v._2)
    //val mediaMap = mediaRDD.map(r => (r(0), Map((r(2), r(5))))).reduceByKey((a, b) => a ++ b).map(v => v._1 -> v._2.mapValues(r => r))
    //mediaMap foreach println

    //val reducedListsMap = mediaMap.map(r => (r._1, r._2.reduceLeft((a,b) => (a._1, a._2.asInstanceOf[Float] + b._2.asInstanceOf[Float]))))
    //val blah = mediaRDD.groupBy(_(0)).mapValues(r => r.map(v => (v(4), v(5))).groupBy(_._1).mapValues(v => v.map(_._2)))//.take(5)//.foreach(println)
    //val blah2 = blah.mapValues(v => v.values.sum[Float])
    // -------------------------------------------------------------------------------------
  }
}
