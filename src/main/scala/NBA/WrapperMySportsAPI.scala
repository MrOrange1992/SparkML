package NBA

import java.net.{HttpURLConnection, URL}
import java.util.Base64
import scala.io.Source


class WrapperMySportsAPI extends Serializable
{
  //username and password for API requests
  val apiKey: String = getProperty("username").get + ":" +  getProperty("password").get

  //base URL path for API pull request
  val pullURL: String = "https://api.mysportsfeeds.com/v1.2/pull/nba/"



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
  * @param params     pull url
  * @return           List of lines from csv stream
  */
def getHttpRequest(params: String): List[String] =
{
  try
  {
    //API DOC
    //https://www.mysportsfeeds.com/data-feeds/api-docs#

    val url = pullURL + params

    //println(url)

    val encoding = Base64.getEncoder.encodeToString(apiKey.getBytes)
    val connection = new URL(url).openConnection.asInstanceOf[HttpURLConnection]
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



}





