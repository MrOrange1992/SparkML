package NBA

//
/**
  * Class for NBA Players to transform csv data to DataFrame
  *
  * @param ID
  * @param lastName
  * @param firstName
  * @param team
  * @param position
  * @param height
  * @param weight
  * @param pointsPG
  * @param assistsPG
  * @param reboundsPG
  * @param fgPct
  * @param ftPct
  * @param minSecPG
  * @param gamesPlayed
  */
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