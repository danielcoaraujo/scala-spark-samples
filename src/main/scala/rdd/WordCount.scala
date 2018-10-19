package rdd

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext

object WordCount {

	def main(args: Array[String]): Unit = {
		Logger.getLogger("org").setLevel(Level.ERROR)

		val sc = new SparkContext("local[*]", "WordCount")
		val lines = sc.textFile("./resources/book.txt")

		val words = lines
			.flatMap(word => word.toLowerCase.split("\\W+"))
			.countByValue()
			.toSeq
		words.sortBy(map => map._2).foreach(println)


//		//Another approach, check if this approach is better or not.
//		val words = lines.flatMap(word => word.toLowerCase.split("\\W+"))
//		val wordsCount = words.map(word => (word,1)).reduceByKey((x,y) => x + y)
//		val sortedWords = wordsCount.map(x => (x._2,x._1)).sortByKey().collect
//		sortedWords.map(map => (map._2, map._1)).foreach(println)
	}

}
