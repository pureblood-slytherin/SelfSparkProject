package Excercises

import Excercises.HelperObjects.{ movies, ratings, users}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

import java.sql.{ Timestamp}
import java.nio.charset.CodingErrorAction
import scala.io.{Codec, Source}

object Solutions {
  val spark = SparkSession.builder()
    .appName("Exercise No. 1")
    .master("local")
    .getOrCreate()

  val sc = spark.sparkContext

  // Handle character encoding issues: (Copied from Internet)
  implicit val codec = Codec("UTF-8")
  codec.onMalformedInput(CodingErrorAction.REPLACE)
  codec.onUnmappableCharacter(CodingErrorAction.REPLACE)

  // Reading file Movies to RDD by two different ways
  //For ref # MovieID::Title::Genres
  // Movies RDD Method 1
  val moviesRDD1 = sc.textFile("src/main/resources/Data/ml-1m/movies.dat")
    .map (lines => lines.split("::"))
    .map(tokens => movies(tokens(0).toInt,tokens(1),tokens(2)))

  // Movies RDD Method 2
  val moviesSeq: Seq[movies] = Source.fromFile("src/main/resources/Data/ml-1m/movies.dat")
    .getLines()
    .map(line => line.split("::"))
    .map(tokens => movies(tokens(0).toInt,tokens(1),tokens(2)) )
    .toList
  val MoviesRDD: RDD[movies] = sc.parallelize(moviesSeq)

  // RDD to DataSet  (Not required just for practice)
  import spark.implicits._
  val MoviesDS = spark.createDataset(moviesRDD1)
  val MoviesDF = MoviesRDD.toDF

  // Reading Ratings RDD
  //UserID::MovieID::Rating::Timestamp
  val RatingsRDD = sc.textFile("src/main/resources/Data/ml-1m/ratings.dat")
    .map (lines => lines.split("::"))
    .map(tokens => (ratings(tokens(0).toInt,tokens(1).toInt,tokens(2).toDouble, new Timestamp(tokens(3).toLong))))

  // Reading users RDD
  //UserID::Gender::Age::Occupation::Zip-code
  val UsersRDD = sc.textFile("src/main/resources/Data/ml-1m/users.dat")
    .map (lines => lines.split("::"))
    .map(tokens => users(tokens(0).toInt,tokens(1),tokens(2).toInt, tokens(3).toInt,tokens(4).toLong) )

  // Exercise 1
  // Solutions by DFs
  val RatingDF = RatingsRDD.toDF.groupBy(col("MovieId"))
    .agg(count("USERId").as("count"),avg(col("userRating")).as("Avg_Rat"))

  val MovieNameRating = RatingDF.join(MoviesDF,"MovieId")
    .where(col("count")>100)    // For removing movies which are rated by very few users and has high ratings
    .orderBy(col("Avg_Rat").desc).toDF()


  // Solutions by RDDs

  val PairRDD = RatingsRDD.map(rating => (rating.MovieId,rating.userRating))

  val groupedRDD = PairRDD.groupByKey().mapValues(movie => (movie.size,(movie.sum/movie.size).toDouble))

  val MoviesKeyRDD = MoviesRDD.map(movie => (movie.MovieID,movie.Title))
  val joinedRDD = MoviesKeyRDD
    .join(groupedRDD)
    .map{
    case ((inInt,(inString,(total,rating))) )=>  (inInt,inString,total,rating)    // Making the RDD presentable
    }
    .filter{ case (a,b,user,c)  => user > 100}// For removing movies which are rated by very few users and has high ratings
  .sortBy(a=> (a._4*(-1)))

  //joinedRDD.toDF("MovieId","Title","UserCount","AvgRating").orderBy(col("AvgRating").desc)show()

  // Exercise 2


  // Reading File
  val MoviesRDD1: RDD[movies] = sc.textFile("src/main/resources/Data/ml-1m/movies.dat")
    .map (lines => {
      val tokens = lines.split("::")
      movies(tokens(0).toInt,tokens(1),tokens(2))
    }
    )

  // Solutions by DF
  val GenreArrayRDD = MoviesRDD1.map(movie =>{
    val arrayGenre = movie.Genres.split('|')
    (movie.Title,arrayGenre)
  }) // Can also be read directly as DF but doing it for practice

  GenreArrayRDD.toDF("MovieId","Genre")
    .select(col("MovieId"),explode(col("Genre"))
      .as("GenreInd")).groupBy(col("GenreInd"))
    .count()


  // Solution by RDD
  val GenreArrayRDD1 = MoviesRDD1.flatMap(movie =>
    movie.Genres.split('|')
      .map(str => (str,movie.MovieID))).groupByKey()
    .mapValues(movie=>movie.size)

  // Exercise 3
  // Solution by DF

  def addRank(DF : DataFrame) ={
    val NewDF = DF.withColumn("new_column",lit("ABC"))
    val w = Window.partitionBy("new_column").orderBy(lit('A'))
    val FinalDF = NewDF.withColumn("Rank", row_number().over(w)).drop("new_column")
    FinalDF
  }
  val MovieNameRating1 = MovieNameRating
    .drop("Genres","count")
  val FInalMovieResults = addRank(MovieNameRating1)

  // solution by RDD

  val FinalMovieRDD = joinedRDD.map{
    case (movieId,title,count,rating) => (movieId,title,rating)
  }.sortBy(a=> (a._3*(-1))).zipWithIndex.map{case (a,i) => (a._1,a._2,a._3,i+1)}

  def main(args: Array[String]): Unit = {

    // Exercise 1 Answer
    joinedRDD.coalesce(1).saveAsTextFile("src/main/resources/Data/Saved/movies1.csv")

    // Exercise 2 Answer
    GenreArrayRDD1.coalesce(1).saveAsTextFile("src/main/resources/Data/Saved/movies2.csv")

    //Exercise 3 Answer
    FinalMovieRDD.coalesce(1).saveAsTextFile("src/main/resources/Data/Saved/movies3.csv")

  }

}
