package stream

import org.apache.log4j.{Level, LogManager, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.twitter.TwitterUtils

object TwitterPopularHashtags {

    def main(args: Array[String]): Unit = {
        val log = LogManager.getRootLogger
        log.info("Starting project: \n")

        setupTwitter()

        val sparkConf = new SparkConf()
            .setAppName("TwitterPopularHashtags")
            .setMaster("local[*]")
        val ssc = new StreamingContext(sparkConf, Seconds(2))

        val stream = TwitterUtils.createStream(ssc, None)

        val messages = stream.map(_.getText)
        val words = messages.flatMap(_.split(" "))
        val hashtags = words.filter(_.startsWith("#"))
        val hashTagsMap = hashtags.map((_, 1))
        val hashAndCount = hashTagsMap.reduceByKeyAndWindow(_ + _, _ - _, Seconds(300), Seconds(2))
        val results = hashAndCount.transform(_.sortBy(_._2, false))
        results.print()

        ssc.checkpoint("./checkpoint/")
        ssc.start()
        ssc.awaitTermination()
    }

    def setupTwitter()= {
        System.setProperty("twitter4j.oauth.consumerKey", "")
        System.setProperty("twitter4j.oauth.consumerSecret", "")
        System.setProperty("twitter4j.oauth.accessToken", "")
        System.setProperty("twitter4j.oauth.accessTokenSecret", "")
    }
}
