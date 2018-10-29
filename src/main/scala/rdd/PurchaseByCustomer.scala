package rdd

import org.apache.spark.SparkContext

object PurchaseByCustomer {

    def main(args: Array[String]): Unit = {
        val sc = new SparkContext("local[*]","PurchaseByCustomer")

        //customerid, itemid, amountspend
        val lines = sc.textFile("./src/main/data/customer-orders.csv")
            .map(parseLine)

        val averageAmount = lines
            .mapValues(x => (x,1))
            .reduceByKey((x,y) => (x._1 + y._1, x._2 + y._2))
            .mapValues(x => (x._1,x._1/x._2))
            .sortBy(x => x._2._1)
            .collect()
            .foreach(value => println(s"Id:${value._1}; Total:${value._2._1}; Average:${value._2._2}"))
    }

    def parseLine(line: String): (Int,Float) = {
        val value = line.split(",")
        val customerId = value(0).toInt
        val amount = value(2).toFloat
        (customerId, amount)
    }
}
