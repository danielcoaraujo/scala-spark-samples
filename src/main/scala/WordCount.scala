import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext

object WordCount {

	def main(args: Array[String]): Unit = {
		Logger.getLogger("WordCount").setLevel(Level.ERROR)

		val sc = new SparkContext("local[*]", "WordCount")
		val lines = sc.textFile("./resources/book.txt")

		val words = lines
			.flatMap(word => word
					.toLowerCase
					.split("\\W")
					.filter(word => word != ""))
			.countByValue()
			.toSeq

		words
			.sortBy(map => map._2)
			.foreach(println)
	}

}
