package DecisionTrees

import java.net.{HttpURLConnection, URL}
import java.util.Base64
import scala.io.Source


class WrapperMySportsAPI extends Serializable
{
  //username and password for API requests
  val apiKey: String = getProperty("username").get + ":" +  getProperty("password").get

  //base URL path for API pull request
  val pullURL: String = "https://api.mysportsfeeds.com/v1.1/pull/nba/"

  //request only needed stats from API
  val playerStatsRequest: String = "?playerstats=MIN/G,PTS/G,AST/G,REB/G,FT%25,FG%25"



  /***
    * Get username and password from properties file
    * @param property   Property field to search
    * @return           Some if property was found else
    */
  def getProperty(property: String): Option[String] =
  {
    Source.fromInputStream(getClass.getResourceAsStream("/sportsAPI.properties"))
      .getLines.find(_.startsWith(property)).map(_.replace(property + "=", ""))
  }


  /***
    * Request csv file from API and return List of String
    * @param season     season for cumulative player stats
    * @return           List of lines from csv stream
    */
  def getPlayerStatsOfSeason(season: String): List[String] =
  {
    try
    {
      //API DOC
      //https://www.mysportsfeeds.com/data-feeds/api-docs#
      val url = new URL(pullURL + season + "/cumulative_player_stats.csv" + playerStatsRequest)

      val encoding = Base64.getEncoder.encodeToString(apiKey.getBytes)
      val connection = url.openConnection.asInstanceOf[HttpURLConnection]
      connection.setRequestMethod("GET")
      connection.setDoOutput(true)
      connection.setRequestProperty("Authorization", "Basic " + encoding)
      val content = connection.getInputStream

      import java.io.{BufferedReader, InputStreamReader}
      val reader = new BufferedReader(new InputStreamReader(content))

      Stream.continually(reader.readLine()).takeWhile(_ != null).toList
    }
    catch { case e: Exception => e.printStackTrace(); List()}
  }


  /*
                        request csv header columns order
    //------------------------------------------------------------------------------------------------------------------
        1   #Player ID
        2   #LastName
        3   #FirstName
        4   #Jersey Num
        5   #Position
        6   #Height
        7   #Weight
        8   #Birth Date
        9   #Age
        10  #Birth City
        11  #Birth Country
        12  #Rookie
        13  #Team ID
        14  #Team Abbr.
        15  #Team City
        16  #Team Name
        17  #GamesPlayed
        18  #MinSecondsPerGame
        19  #PtsPerGame
        20  #AstPerGame
        21  #RebPerGame
        22  #FtPct
        23  #FgPct
    //------------------------------------------------------------------------------------------------------------------
*/


  /***
    * Map csv data line to Player instance
    * @param line       data line from API csv file
    * @return           Player instance
    */
  def mappPlayerStats(line:String): Player =
  {
    // field: Birth City might contain a comma within quotation marks which should not be delimited
    // https://stackoverflow.com/questions/1757065/java-splitting-a-comma-separated-string-but-ignoring-commas-in-quotes
    val fields = line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)")

    Player(
      fields(1).toInt,      //Player ID
      fields(2),            //Last name
      fields(3),            //First name
      fields(16),           //Team name
      positionToIndex(fields(5)),            //Position
      fields(6)             //Height in feet
        .replace("\"", "")
        .replace("\'", ".")
        .toFloat,
      fields(7).toInt,      //Weight in pounds
      fields(19).toFloat,   //Points per game
      fields(20).toFloat,   //Assists per game
      fields(21).toFloat,   //Rebounds per game
      fields(23).toFloat,   //Field goal percentage
      fields(22).toFloat,   //Free throw percentage
      fields(18).toFloat,   //Minutes/Seconds played per game
      fields(17).toInt)     //Games Played
  }

  /***
    * Transform position string to numeric value
    * @return   player position mapped to integer
    */
  def positionToIndex: (String => Int) =
  {
    case "G" => 1
    case "PG" => 1
    case "SG" => 2
    case "F" => 3
    case "SF" => 3
    case "PF" => 4
    case "C" => 5
    case default => 0
  }


}

// case class for NBA Player to transform csv data to DataFrame
case class Player(ID:Int,
                  lastName:String,
                  firstName: String,
                  team: String,
                  position: Int,
                  height: Float,
                  weight: Int,
                  pointsPG: Float,
                  assistsPG: Float,
                  reboundsPG: Float,
                  fgPct: Float,
                  ftPct: Float,
                  minSecPG: Float,
                  gamesPlayed: Int)





