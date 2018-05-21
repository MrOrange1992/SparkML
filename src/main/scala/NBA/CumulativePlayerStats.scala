package NBA

class CumulativePlayerStats extends Serializable
{
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

  val cumulativeString = "/cumulative_player_stats.csv"


  val playerStatsRequest: String = "?playerstats=MIN/G,PTS/G,AST/G,REB/G,FT%25,FG%25"


  /***
    * Map csv data line to Player instance
    * @param line       data line from API csv file
    * @return           Player instance
    */
  def mappPlayerStats(line:String): Option[Player] =
  {
    // field: Birth City might contain a comma within quotation marks which should not be delimited
    // https://stackoverflow.com/questions/1757065/java-splitting-a-comma-separated-string-but-ignoring-commas-in-quotes
    val fields = line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)")

    if (fields(6).equals(""))
      None
    else

      Some(Player(
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
      )
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
