package dataset

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object FriendsByAge {

    case class Person(id: Int, name: String, age: Int, numFriends: Int)

    def parseLine(line : String) = {
        val fields = line.split(",")
        val id = fields(0).toInt
        val name = fields(1)
        val age = fields(2).toInt
        val numFriends = fields(3).toInt
        Person(id, name, age, numFriends)
    }

    def main(args: Array[String]): Unit = {
        val spark = SparkSession.builder()
            .appName("FriendsByAge")
            .master("local[*]")
            .getOrCreate()

        val lines = spark.sparkContext.textFile("./resources/fakefriends.csv")

        import spark.implicits._
        val people = lines.map(parseLine).toDS.cache()

        people.printSchema()

        people.select("name").show()

        people.filter(people("age") < 21).show()

        people.groupBy("age").count().show()

        people.select(people("name"), people("age") + 10).show()

        spark.stop()
    }
}
