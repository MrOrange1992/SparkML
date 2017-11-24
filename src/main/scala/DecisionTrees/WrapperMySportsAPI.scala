package DecisionTrees

import java.net.{HttpURLConnection, URL}
import java.util.Base64

import scala.io.Source


class WrapperMySportsAPI extends Serializable
{
  val basePath: String = "https://api.mysportsfeeds.com/v1.1/pull/nba/"

  val apiKey: String = getProperty("username").get + ":" +  getProperty("password").get

  /***
    * Get username and password from properties file
    * @param property
    * @return
    */
  def getProperty(property: String): Option[String] =
  {
    Source.fromInputStream(getClass.getResourceAsStream("/sportsAPI.properties"))
      .getLines.find(_.startsWith(property)).map(_.replace(property + "=", ""))
  }


  def getStatsOfSeason(season: String): List[String] =
  {
    try
    {
      //API DOC
      //https://www.mysportsfeeds.com/data-feeds/api-docs#
      val url = new URL(basePath + season + "/cumulative_player_stats.csv")

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

  def mapper(line:String): Player =
  {
    // field: Birth City might contain a comma within quotation marks which should not be delimited
    // https://stackoverflow.com/questions/1757065/java-splitting-a-comma-separated-string-but-ignoring-commas-in-quotes
    val fields = line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)")

    Player(
      fields(1).toInt,      //Player ID
      fields(2),            //Last name
      fields(3),            //First name
      fields(16),           //Team name
      fields(5),            //Position
      fields(6).replace("\"", "").replace("\'", ".").toFloat,   //Height in feet
      fields(7).toInt,      //Height in pounds
      fields(47).toFloat,   //Points per game
      fields(45).toFloat,   //Assists per game
      fields(43).toFloat,   //Rebounds per game
      fields(32).toFloat,   //Field goal percentage
      fields(37).toFloat,   //Free throw percentage
      fields(61).toFloat)   //Minutes/Seconds played per game
  }

}

case class Player(ID:Int,
                  lastName:String,
                  firstName: String,
                  team: String,
                  position: String,
                  height: Float,
                  weight: Int,
                  pointsPG: Float,
                  assistsPG: Float,
                  reboundsPG: Float,
                  fgPct: Float,
                  ftPct: Float,
                  minSecPG: Float)





/*
                        General
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
                        Field Goals
18  #Fg2PtAtt
19  #Fg2PtAttPerGame
20  #Fg2PtMade
21  #Fg2PtMadePerGame
22  #Fg2PtPct
23  #Fg3PtAtt
24  #Fg3PtAttPerGame
25  #Fg3PtMade
26  #Fg3PtMadePerGame
27  #Fg3PtPct
28  #FgAtt
29  #FgAttPerGame
30  #FgMade
31  #FgMadePerGame
32  #FgPct
                        Free Throws
33  #FtAtt
34  #FtAttPerGame
35  #FtMade
36  #FtMadePerGame
37  #FtPct
                        Rebounds
38  #OffReb
39  #OffRebPerGame
40  #DefReb
41  #DefRebPerGame
42  #Reb
43  #RebPerGame
                        Assists
44  #Ast
45  #AstPerGame
                        Points
46  #Pts
47  #PtsPerGame
                        Turnovers
48  #Tov
49  #TovPerGame
                        Steals
50  #Stl
51  #StlPerGame
                        Blocks
52  #Blk
53  #BlkPerGame

#BlkAgainst,#BlkAgainstPerGame,#FoulPers,#FoulPersPerGame,#PlusMinus,#PlusMinusPerGame,

60  #MinSeconds
61  #MinSecondsPerGame

#Fouls,#FoulsPerGame,#FoulsDrawn,#FoulsDrawnPerGame,#FoulPersDrawn,#FoulPersDrawnPerGame,#FoulTech,#FoulTechPerGame,#FoulTechDrawn,#FoulTechDrawnPerGame,#FoulFlag1,#FoulFlag1PerGame,#FoulFlag1Drawn,#FoulFlag1DrawnPerGame,#FoulFlag2,#FoulFlag2PerGame,#FoulFlag2Drawn,#FoulFlag2DrawnPerGame,#Ejections
*/