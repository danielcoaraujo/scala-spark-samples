import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext

object WordCount {

	def main(args: Array[String]): Unit = {
		Logger.getLogger("WordCount").setLevel(Level.ERROR)

		val sc = new SparkContext("local[*]", "WordCount")
		val lines = sc.textFile("./resources/book.txt")

		val words = lines
			.flatMap(word => word.toLowerCase.split("\\W+"))
			.countByValue()
			.toSeq

		words
			.map(map => (map._2, map._1))
			.sorted
			.foreach(println)
	}

}
