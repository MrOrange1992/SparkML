package MedienTransparenz

import org.apache.log4j._
import org.apache.spark._

/*
  *  Data set: https://github.com/mihi-tr/dpkg-medientransparenz/blob/master/data/mtg.csv
  *
  *  Line fields with ',' delimiter:
  *  0                    1      2            3           4       5                       6    7           8  9
  *  MEDIUM_MEDIENINHABER,medium,RECHTSTRÄGER,LEERMELDUNG,QUARTAL,bekanntgabe_description,year,BEKANNTGABE,id,EURO
  *
  *  For this dataset:
    *  year(6) contains only 2012
    *  QUARTAL(4) contains only 20123
  */

object MediaAnalysisRDD
{
  //case class MediaDataRow(ID:Int, medienInhaber:String, medium:String, Rechtsträger:String)

  // Custom Type for dataRow
  // Key-Value Pair of mapping fields
  //                8       0       1      2      3    4     5      6    7    9
  ///type MediaRow = (Int, (String, String, String, Int, Int, String, Int, Int, Int))
  //type MediaSet = Map[MediaRow]

  /***
    * Map .csv lines to RDD
    * @param line
    * @return
    */
  def mapper(line: String) : (String, Float) =
  {
    val fields = line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)")

    (fields(1), fields(9).toFloat)
  }

  /*
      TODO
  def mapAll(rdd: RDD[String]) : DataSet =
  {
    val fields = line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)")
  }
  */


  def main(args: Array[String]): Unit =
  {
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Spark Context, using all Cores of local machine
    val sc = new SparkContext("local[*]", "MediaAnalysisRDD")

    //TODO fix first line is header
    // .csv data to spark context
    val lines = sc.textFile("./data/medienData.csv").filter(x=> !x.contains("MEDIUM_MEDIENINHABER"))

    // map raw data to RDD
    val mediaMoney = lines.map(mapper)

    // reduce by key "medium"
    val uniqueMedia = mediaMoney.reduceByKey((x, y) => x + y)
    //uniqueMedia foreach println

    // filter all greater than 20000
    val filteredMedia = uniqueMedia.filter(x => x._2 > 20000)
    //filteredMedia foreach println

    filteredMedia.sortBy(- _._2) foreach println

    /*
    // flip key value pair for sortByKey function
    val flippedMedia = filteredMedia.map( x => (x._2, x._1) )

    val sortedMedia = flippedMedia.sortByKey()

    sortedMedia.foreach(l => println(s"${l._2}: ${l._1} Euro"))

    //sortedMedia foreach println
*/
  }

}
