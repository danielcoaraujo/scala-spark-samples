package stream

import org.apache.log4j.{Level, LogManager, Logger}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.twitter._
import com.typesafe.config._

object PopularHashtags {

    def main(args: Array[String]): Unit = {
        val log = LogManager.getRootLogger
        log.info("Starting project: \n")

        TwitterConfig.setupTwitter

        val ssc = new StreamingContext("local[*]", "PopularHashtags", Seconds(1))
        Logger.getLogger("org").setLevel(Level.ERROR)

        val stream = TwitterUtils.createStream(ssc, None)
        val hashtags = stream.flatMap(status => status.getText.split(" ").filter(_.startsWith("#")))

        val topCounts10 = hashtags.map((_, 1)).reduceByKeyAndWindow(_ + _, Seconds(10))
            .map{case (topic, count) => (count, topic)}
            .transform(_.sortByKey(false))

        topCounts10.foreachRDD(rdd => {
            val topList = rdd.take(10)
            println("\nPopular topics in last 10 seconds (%s total):".format(rdd.count()))
            topList.foreach{case (count, tag) => println("%s (%s tweets)".format(tag, count))}
        })

        ssc.start()
        ssc.awaitTermination()
    }
}
