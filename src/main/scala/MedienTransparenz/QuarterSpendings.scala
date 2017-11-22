package MedienTransparenz

import org.apache.log4j._
import org.apache.spark.sql.SparkSession
import scala.util.Try

object QuarterSpendings
{
  case class MediaDataRow(Quartal: Int, medium_medienInhaber:String,  Euro: Int)

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

      MediaDataRow(fields(1).toInt,
        Try { fields(4) }.getOrElse(""),        //handle null fields
        Try { fields(5).toInt }.getOrElse(0)) //handle null fields
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


    //mediaDF.printSchema()
    //mediaDF.show()

    //val mediaDFQuarters = mediaDF.filter(mediaDF("QUARTAL") === "20121")


    val mediaDFQ1 = mediaDF.filter($"QUARTAL" % 10 === 1).groupBy($"QUARTAL")//.sum("EURO")//.sort(- $"sum(EURO)")

    //mediaDFQ1.show()


    //mediaDF.filter(mediaDF("medium_medienInhaber") === "Servus TV").groupBy($"QUARTAL").sum("EURO").sort($"QUARTAL").show()

  }

}


