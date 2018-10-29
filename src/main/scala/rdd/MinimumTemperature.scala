package rdd

import org.apache.spark.SparkContext

object MinimumTemperature {

	def main(args: Array[String]): Unit = {
		val sc = new SparkContext("local[*]", "MinimumTemperature")
		val parsedLines = sc.textFile("./src/main/data/1800.csv").map(parseLines)

		val minimumTemperature = parsedLines
			.filter(line => line._2 == "TMIN")
			.map(line => (line._1, line._3))

		val minimumTemperaturePerLocation = minimumTemperature
			.reduceByKey((x, y) => min(x, y))
			.collect()
			.map(printTemperature)

//		printTemperature(minimumTemperature)
	}

	def parseLines(line : String) = {
		val fields = line.split(",")
		val stationId = fields(0)
		val entryType = fields(2)
		val temperature = fields(3).toInt
		(stationId, entryType, temperature)
	}

	def min(temp1: Int, temp2: Int): Int = {
		if(temp1 < temp2) temp1 else temp2
	}

	def printTemperature(line: (String, Int)) = {
		val stationId = line._1
		val temperature = line._2
		println(s"StationId:$stationId => Minimum Temperature:$temperature")
	}

//	def printTemperature(minimumTemperature: RDD[(String, Int)]) = {
//		minimumTemperature.foreach(line => {
//			val stationId = line._1
//			val temperature = line._2
//			println(s"Station: $stationId; Minimum Temperature: $temperature")
//		})
//	}

}