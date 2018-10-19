package rdd

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext

object FriendsByAge {

    def parseLine(line : String) = {
        val fields = line.split(",")
        val age = fields(2).toInt
        val numFriends = fields(3).toInt
        (age, numFriends)
    }

    def main(args: Array[String]): Unit = {
        Logger.getLogger("org").setLevel(Level.ERROR)

        val sc = new SparkContext("local[*]", "../resources/fakefriends.csv")

        val lines = sc.textFile("./resources/fakefriends.csv")

        val mapAgeNumFriends = lines.map(parseLine)

        //(age, (numFriends, 1))
        val totalsByAge = mapAgeNumFriends.mapValues(x => (x,1)).reduceByKey((x,y) => (x._1 + y._1, x._2 + y._2) )

        //(age, (numFriendsTotal, totalCount))
        val map_ageAverageFriends = totalsByAge.mapValues(line => line._1/line._2)

        map_ageAverageFriends
            .collect()
            .sortBy(x => x._2)
            .foreach(println)
    }
}