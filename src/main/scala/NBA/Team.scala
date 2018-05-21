package NBA

case class Team(     //ID:Int,
                //name: String,
                //Fg2PtAtt: Int,    //2PA
                Fg2PtMade: Int,   //2PM
                Fg2PtPct: Float,  //2P%
                //Fg3PtAtt: Int,    //3PA
                Fg3PtMade: Int,   //3PM
                Fg3PtPct: Float,  //3P%
                //FgAtt: Int,       //FGA
                FgMade: Int,      //FGM
                FgPct: Float,     //FG%
                FtAtt: Int,       //FTA
                FtMade: Int,      //FTM
                FtPct: Float,     //FT%
                OffReb: Int,      //OREB
                DefReb: Int,      //DREB
                //Reb: Int,         //REB
                Ast: Int,         //AST
                Pts: Int,         //PTS
                Tov: Int,         //TOV
                Stl: Int,         //STL
                Blk: Int,         //BS
                BlkAgainst: Int,  //BA
                PtsAgainst: Int,  //PTSA
                Fouls: Int,       //F
                FoulsDrawn: Int,  //FD
                //FoulPers: Int,    //PF
                //FoulPersDrawn: Int, //PFD
                PlusMinus: Int,   //+/-             --> probably strong indicator ----> is strongest indicator :D
                WinLoss: Int)     //W               --> Label

