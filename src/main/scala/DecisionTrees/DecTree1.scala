package DecisionTrees

import org.apache.log4j._
import org.apache.spark.sql.SparkSession

object DecTree1
{

  /***
    * Class for NBA Player
    * @param ID
    * @param lastName
    * @param firstName
    * @param team           Name of last joined team of season
    * @param position       PointGuard, ShootingGuard, SmallForward, PowerForward, Center
    * @param pointsPG       Points per game average
    * @param assistsPG      Assists per game average
    * @param reboundsPG     Rebounds per game average
    */
  case class Player(ID:Int, lastName:String, firstName: String, team: String, position: String, pointsPG: Float,
                    assistsPG: Float, reboundsPG: Float)




  /***
    * Map lines to RDD
    * @param line
    * @return
    */
  def mapper(line:String): Player =
  {
    // field: Birth City might contain a comma within quotation marks which should not be delimited
    // https://stackoverflow.com/questions/1757065/java-splitting-a-comma-separated-string-but-ignoring-commas-in-quotes
    val fields = line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)")

    Player(fields(1).toInt, fields(2), fields(3), fields(16), fields(5), fields(47).toFloat, fields(45).toFloat, fields(43).toFloat)
  }





  def main(args: Array[String]): Unit =
  {
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)
    // Use new SparkSession interface in Spark 2.0
    val spark = SparkSession.builder.appName("DecTree1").master("local[*]").getOrCreate()

    /*
      Currently Available
        2015-2016-regular
        2016-playoff
        2016-2017-regular
        2017-playoff
     */
    val apiWrapper = new WrapperMySportsAPI
    val s2016PlayerStats = apiWrapper.getStatsOfSeason("2016-playoff")

    // Convert http-request-stream List[String] to a DataSet
    import spark.implicits._
    val lines = spark.sparkContext.parallelize(s2016PlayerStats.tail)   //tail because first line is csv header
    val players = lines.map(mapper).toDS()//.cache()
    //players.printSchema()
    //players.show()

    //Lebron James
    //players.filter(players("lastName") === "James").show()

    //Cleveland Cavaliers
    players.filter(players("team") === "Cavaliers").show()


    //stop session
    spark.stop()
  }

}
