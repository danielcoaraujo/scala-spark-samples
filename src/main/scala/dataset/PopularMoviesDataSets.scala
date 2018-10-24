package dataset

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object PopularMoviesDataSets {
	case class Movie(id: Int, personId: Int, rating: Int, timestamp: Int)

	def parseToMovie(line: String) : Movie = {
		val fields = line.split("\t")
        val id = fields(1).toInt
        val personId = fields(0).toInt
		val rating = fields(2).toInt
		val timestamp = fields(3).toInt
		Movie(id, personId, rating, timestamp)
	}

	def main(args: Array[String]): Unit = {
		Logger.getLogger("org").setLevel(Level.ERROR)

		val spark = SparkSession.builder()
			.appName("PopularMoviesDataSets")
			.master("local[*]")
			.getOrCreate()

		import spark.implicits._
		val movies = spark.sparkContext
			.textFile("./resources/u.data")
			.map(parseToMovie)
			.toDS()
			.cache()

        val topMovies = movies
            .groupBy("id")
            .count()
            .orderBy($"count".desc)
            .take(10)
            .foreach(println)

        val mapIdName = loadMovieNames(spark)

//        for(movie <- topMovies){
//            println(movie(0), mapIdName.asInstanceOf[Int])
//        }

        spark.stop()
    }

    def loadMovieNames(sparkSession: SparkSession) = {
        val lines = sparkSession
            .sparkContext
            .textFile("./resources/u.item")
            .map(line => {
                val fields = line.split("\\|")
                val id = fields(0).toInt
                val name = fields(1)
                (id, name)
            }).collect().toMap
    }
}
