package stream

import org.apache.log4j.{Level, LogManager, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.twitter._

object PopularHashtags {

    def main(args: Array[String]): Unit = {
        val log = LogManager.getRootLogger
        log.info("Starting project: \n")

        setupTwitter

//        val sparkConf = new SparkConf()
//            .setAppName("TwitterPopularHashtags")
//            .setMaster("local[2]")
//        val ssc = new StreamingContext(sparkConf, Seconds(1))

        val ssc = new StreamingContext("local[*]", "PopularHashtags", Seconds(1))
        Logger.getLogger("org").setLevel(Level.ERROR)

        val stream = TwitterUtils.createStream(ssc, None)

//        val messages = stream.map(_.getText)
//        val words = messages.flatMap(_.split(" "))
//        val hashtags = words.filter(_.startsWith("#"))
//        val hashTagsMap = hashtags.map((_, 1))
//        val hashAndCount = hashTagsMap.reduceByKeyAndWindow(_ + _, _ - _, Seconds(300), Seconds(1))
//        val results = hashAndCount.transform(_.sortBy(_._2, false))
//        results.print()

        val hashtags = stream.flatMap(status => status.getText.split(" ").filter(_.startsWith("#")))

        val topCounts10 = hashtags.map((_, 1)).reduceByKeyAndWindow(_ + _, Seconds(10))
            .map{case (topic, count) => (count, topic)}
            .transform(_.sortByKey(false))

        topCounts10.foreachRDD(rdd => {
            val topList = rdd.take(10)
            println("\nPopular topics in last 10 seconds (%s total):".format(rdd.count()))
            topList.foreach{case (count, tag) => println("%s (%s tweets)".format(tag, count))}
        })

        ssc.checkpoint("./checkpoint/")
        ssc.start()
        ssc.awaitTermination()
    }

    def setupTwitter()= {
        System.setProperty("twitter4j.oauth.consumerKey", "eW4DGocQmaFNybe5oiHsK2gzN")
        System.setProperty("twitter4j.oauth.consumerSecret", "dONYQG2uq5IEQfIPwioUTzcKrWZ4yMbmUYQ1JhKYpVXaH9NLQx")
        System.setProperty("twitter4j.oauth.accessToken", "1054930223794475008-h48ArjI953ySBeNIJnmXOFf8RwFNi3")
        System.setProperty("twitter4j.oauth.accessTokenSecret", "m5sN3Yw44iZe95SFPVgbq8dKGBuhexhHNUoESRIAgqkNo")
    }
}
