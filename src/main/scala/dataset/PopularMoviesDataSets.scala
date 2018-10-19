package dataset

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object PopularMoviesDataSets {
	case class Movie(id: Int, personId: Int, rating: Int, timestamp: Int)

	def parseToMovie(line: String) : Movie = {
		val fields = line.split("\t")
		val id = fields(0).toInt
		val personId = fields(1).toInt
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

		movies.printSchema()
	}
}
