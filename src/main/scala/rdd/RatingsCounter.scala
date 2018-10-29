package rdd

import org.apache.spark.SparkContext

object RatingsCounter {

  def main(args: Array[String]): Unit = {
    val sc = new SparkContext("local[*]", "RatingsCounter")

    val lines = sc.textFile("./src/main/data/u.data")

    val ratings = lines.map(x => x.toString.split("\t")(2))

    val results = ratings.countByValue()

    val sortedResults = results.toSeq.sortBy(_._1)

    sortedResults.foreach(println)
  }
}