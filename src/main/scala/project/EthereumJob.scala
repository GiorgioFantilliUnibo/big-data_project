package lab104

import org.apache.spark.sql.SparkSession
import utils._
import org.apache.spark.sql.SaveMode
import org.apache.spark.{HashPartitioner, SparkConf}

object MovieLens {

  val path_to_datasets = "/datasets/big/"

  val path_ml_movies = path_to_datasets + "ml-movies.csv"
  val path_ml_ratings = path_to_datasets + "ml-ratings.csv"
  val path_ml_tags = path_to_datasets + "ml-tags.csv"

  val path_output_avgRatPerMovie = "/output/avgRatPerMovie"
  val path_output_avgRatPerGenre = "/output/avgRatPerGenre"

  def main(args: Array[String]): Unit = {
    // Create a SparkConf object; the configuration settings you put here will override those given in the Run/Debug configuration
    val sparkConf = new SparkConf()
      .setAppName("MovieLens job")
//      .set("spark.executor.memory", "4g")
//      .set("spark.executor.cores", "4")
//      .set("spark.executor.instances", "2")
    val spark = SparkSession.builder.config(sparkConf).getOrCreate()

    val sqlContext = spark.sqlContext // needed to save as CSV
    import sqlContext.implicits._

    if(args.length < 2){
      println("The first parameter should indicate the deployment mode (\"local\" or \"remote\")")
      println("The second parameter should indicate the job: "
        + "1 for AvgRating by Movie (join-and-agg), "
        + "2 for AvgRating by Movie (agg-and-join), "
        + "3 for AvgRating by Movie (agg-and-bjoin), "
        + "4 for AvgRating by Genre (join-and-agg), "
        + "5 for AvgRating by Genre (agg-join-agg)")
      return
    }

    val deploymentMode = args(0)
    var writeMode = deploymentMode
    if(deploymentMode == "sharedRemote"){
      writeMode = "remote"
    }
    val job = args(1)

    // Initialize input
    val rddMovies = spark.sparkContext.
      textFile(Commons.getDatasetPath(deploymentMode, path_ml_movies)).
      flatMap(MovieLensParser.parseMovieLine)
    val rddRatings = spark.sparkContext.
      textFile(Commons.getDatasetPath(deploymentMode, path_ml_ratings)).
      flatMap(MovieLensParser.parseRatingLine)
    val rddTags = spark.sparkContext.
      textFile(Commons.getDatasetPath(deploymentMode, path_ml_tags)).
      flatMap(MovieLensParser.parseTagLine)


    val rddMoviesKV = rddMovies.map(x => (x._1,x._2))

    // Two alternatives to get an RDD with (movieId, genre) tuples
    // (not used in the jobs, but just for demo)
    val rddMoviesByGenresKV = rddMovies.
      map(x => (x._1,x._3)).
      flatMapValues(x => x.split('|'))
    val rddMoviesByGenresKV2 = rddMovies.
      flatMap({case(id,tit,gen) => gen.split('|').map(g => (id,g))})

    if (job=="1"){
      rddRatings.
        map(x => ((x._2),(x._3))).
        join(rddMoviesKV).
        map({case (m,(r,t)) => ((m,t),(r,1))}).
        reduceByKey((v1,v2) => (v1._1+v2._1, v1._2+v2._2)).
        map({case ((m,t),(sum,cnt)) => (m, t, sum/cnt, cnt)}).
        coalesce(1).
        toDF().write.format("csv").mode(SaveMode.Overwrite).
        save(Commons.getDatasetPath(writeMode,path_output_avgRatPerMovie))
    }
    else if (job=="2"){
      rddRatings.
        map(x => (x._2,(x._3,1))).
        reduceByKey((v1,v2) => (v1._1+v2._1, v1._2+v2._2)).
        mapValues({case (sum,cnt) => (sum/cnt, cnt)}).
        join(rddMoviesKV).
        map({case (m,((r,cnt),t)) => (m,t,r,cnt)}).
        coalesce(1).
        toDF().write.format("csv").mode(SaveMode.Overwrite).
        save(Commons.getDatasetPath(writeMode,path_output_avgRatPerMovie))
    }
    else if (job=="3"){
      val bRddMovies = spark.sparkContext.broadcast(rddMoviesKV.collectAsMap())
      rddRatings.
        map(x => ((x._2),(x._3,1))).
        reduceByKey((v1,v2) => (v1._1+v2._1, v1._2+v2._2)).
        mapValues({case (sum,cnt) => (sum/cnt, cnt)}).
        map({case (m,(r,cnt)) => (m,bRddMovies.value.get(m),r,cnt)}).
        coalesce(1).
        toDF().write.format("csv").mode(SaveMode.Overwrite).
        save(Commons.getDatasetPath(writeMode,path_output_avgRatPerMovie))
    }
    else if (job=="4"){
      rddRatings.
        map(x => (x._2,x._3)).
        join(rddMoviesKV).
        map(x => (x._2._2,(x._2._1,1))).
        reduceByKey((v1,v2) => (v1._1+v2._1, v1._2+v2._2)).
        map(x => (x._1, x._2._1/x._2._2, x._2._2)).
        coalesce(1).
        toDF().write.format("csv").mode(SaveMode.Overwrite).
        save(Commons.getDatasetPath(writeMode,path_output_avgRatPerGenre))
    }
    else if (job=="5"){
      rddRatings.
        map(x => (x._2,(x._3,1))).
        reduceByKey((v1,v2) => (v1._1+v2._1, v1._2+v2._2)).
        join(rddMoviesKV).
        map(x => (x._2._2,(x._2._1._1,x._2._1._2))).
        reduceByKey((v1,v2) => (v1._1+v2._1, v1._2+v2._2)).
        map(x => (x._1, x._2._1/x._2._2, x._2._2)).
        coalesce(1).
        toDF().write.format("csv").mode(SaveMode.Overwrite).
        save(Commons.getDatasetPath(writeMode,path_output_avgRatPerGenre))
    }
    else {
      println("Wrong job number")
    }

  }

}