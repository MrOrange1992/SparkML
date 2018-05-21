package NBA

class TeamGameLogs extends Serializable
{

  val gameLogsParam: String = "/team_gamelogs.csv"

  val teamNameRequest: String = "?team=cle,bos,mia,mil,dal,hou,lal,phi,tor,sas,gsw,uta,nop,nyk,okl,chi,min,por,ind,mil,was,den,lac,cha,det,phx,atl,orl,bro,mem,sac"


  /***
    * Map csv data line to Team
    * @param line       data line from API csv file
    * @return           Team instance
    */
  def mappGameLog(line:String): Option[Team] =
  {
    // field: Birth City might contain a comma within quotation marks which should not be delimited
    // https://stackoverflow.com/questions/1757065/java-splitting-a-comma-separated-string-but-ignoring-commas-in-quotes
    val fields = line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)")

    try {
      Some(Team(

        //fields(17).toInt,   //2PA
        fields(19).toInt,    //2PM
        fields(21).toFloat,//2P%
        //fields(22).toInt,//3PA
        fields(24).toInt,//3PM
        fields(26).toFloat,//3P%
        //fields(27).toInt,//FGA
        fields(29).toInt,//FGM
        fields(31).toFloat,//FG%
        fields(32).toInt,//FTA
        fields(34).toInt,//FTM
        fields(36).toFloat,//FT%
        fields(37).toInt,//OREB
        fields(39).toInt,//DREB
        //fields(41).toInt,//REB
        fields(43).toInt,//AST
        fields(45).toInt,//PTS
        fields(47).toInt,//TOV
        fields(49).toInt,//STL
        fields(51).toInt,//BS
        fields(53).toInt,//BA
        fields(55).toInt,//PTSA
        fields(57).toInt,//F
        fields(65).toInt,//FD
        //fields(59).toInt,//PF
        //fields(67).toInt,//PFD
        fields(63).toInt,//+/-
        fields(80).toInt))//W
    }

    catch { case e: Exception => None}
  }
}
/*
17 #Fg2PtAtt,
18 #Fg2PtAttPerGame,
19 #Fg2PtMade,
20 #Fg2PtMadePerGame,
21 #Fg2PtPct,
22 #Fg3PtAtt,
23 #Fg3PtAttPerGame,
24 #Fg3PtMade,
25 #Fg3PtMadePerGame,
26 #Fg3PtPct,
27 #FgAtt,
28 #FgAttPerGame,
29 #FgMade,
30 #FgMadePerGame,
31 #FgPct,
32 #FtAtt,
33 #FtAttPerGame,
34 #FtMade,
35 #FtMadePerGame,
36 #FtPct,
37 #OffReb,
38 #OffRebPerGame,
39 #DefReb,
40 #DefRebPerGame,
41 #Reb,
42 #RebPerGame,
43 #Ast,
44 #AstPerGame,
45 #Pts,
46 #PtsPerGame,
47 #Tov,
48 #TovPerGame,
49 #Stl,
50 #StlPerGame,
51 #Blk,
52 #BlkPerGame,
53 #BlkAgainst,
54 #BlkAgainstPerGame,
55 #PtsAgainst,
56 #PtsAgainstPerGame,
57 #Fouls,
58 #FoulsPerGame,
59 #FoulPers,
60 #FoulPersPerGame,
61 #FoulTech,
62 #FoulTechPerGame,
63 #PlusMinus,
64 #PlusMinusPerGame,
65 #FoulsDrawn,
66 #FoulsDrawnPerGame,
67 #FoulPersDrawn,
68 #FoulPersDrawnPerGame,
69 #FoulTechDrawn,
70 #FoulTechDrawnPerGame,
71 #FoulFlag1,
72 #FoulFlag1PerGame,
73 #FoulFlag1Drawn,
74 #FoulFlag1DrawnPerGame,
75 #FoulFlag2,
76 #FoulFlag2PerGame,
77 #FoulFlag2Drawn,
78 #FoulFlag2DrawnPerGame,
79 #Ejections,
80 #Wins,
81 #Losses,
82 #WinPct,
83 #GamesBack
*/