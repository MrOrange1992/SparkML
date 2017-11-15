package MedienTransparenz

import org.apache.log4j._
import org.apache.spark.sql.SparkSession

import scala.util.Try


/*
  *  Data set: https://github.com/AnotherCodeArtist/medien-transparenz.at/blob/master/data/20123-20171-refine-latin1.csv
  *  csv was converted from ; delimiters to , delimiters with Microsoft Excel
  *
  *  Line fields with ',' delimiter:
  *  0            1       2           3           4                     5
  *  RECHTSTR�GER,QUARTAL,BEKANNTGABE,LEERMELDUNG,MEDIUM_MEDIENINHABER,EURO
  */


object MediaAnalysisDataFrame
{

  case class MediaDataRow(Rechtsträger:String, Quartal: Int, Bekanntgabe: Int, Leermeldung: Int, medium_medienInhaber:String,  Euro: Float)

  /***
    * Map .csv lines to RDD
    *
    * @param line
    * @return
    */
  /* TODO
    map with Some/None for invalid data lines instead of try-catch
    then use flatmap on Option set to discard all None values (runtime impact?)
   */
  def mappData(line: String): MediaDataRow =
  {
    try
    {
      //val fields = line.split(',')
      val fields = line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)")

      MediaDataRow(fields(0), fields(1).toInt, fields(2).toInt, fields(3).toInt,
        Try { fields(4) }.getOrElse(""),        //handle null fields
        Try { fields(5).toFloat }.getOrElse(0)) //handle null fields
    }
    catch { case e: Exception => throw new Exception(s"\nError @ mapping data lines. \nCorrupt data in line:    $line")}

  }



  def main(args: Array[String]): Unit =
  {
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Spark Context, using all Cores of local machine
    val spark = SparkSession.builder.appName("MediaAnalysisDataFrame").master("local[*]").getOrCreate()

    // Infer the schema, and register the DataSet as a table.
    import spark.implicits._
    val lines = spark.sparkContext.textFile("./data/MedienTransparenz-20123-20171.csv")
    val mediaDF = lines.map(mappData).toDS().cache()    // use cache() if dataset will be used repeatedly


    mediaDF.printSchema()
    //mediaDF.show()
    //mediaDF.select("ID").show()

    // Sum expenses of Medieninhaber by QUARTAL
    //TODO could be useful for predicting next QUARTAL with MLlib (Linear Regression / Least Squares)
    mediaDF.filter(mediaDF("medium_medienInhaber") === "Servus TV").groupBy($"QUARTAL").sum("EURO").sort($"QUARTAL").show()

  }

}
