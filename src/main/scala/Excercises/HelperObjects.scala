package Excercises

import java.sql.Timestamp

object HelperObjects {
  //MovieID::Title::Genres
  case class movies (MovieID : Int, Title : String,Genres:String)
  //UserID::MovieID::Rating::Timestamp
  case class ratings (UserId : Int, MovieId:Int,userRating: Double, timeStamp: Timestamp)
  //UserID::Gender::Age::Occupation::Zip-code
  case class users (UserID:Int,Gender:String,Age:Int,Occupation:Int,ZipCode:Long)
  val Occupation = Map(
    0 -> "other" ,
    1 -> "academic/educator",
    2 -> "artist",
    3->  "clerical/admin",
    4->  "college/grad student",
    5->  "customer service",
    6->  "doctor/health care",
    7-> "executive/managerial",
    8-> "farmer",
    9-> "homemaker",
    10-> "K-12 student",
    11-> "lawyer",
    12-> "programmer",
    13-> "retired",
    14-> "sales/marketing",
    15-> "scientist",
    16-> "self-employed",
    17-> "technician/engineer",
    18-> "tradesman/craftsman",
    19-> "unemployed",
    20-> "writer"
  )
}
