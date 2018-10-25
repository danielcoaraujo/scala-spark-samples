package stream

import com.typesafe.config.ConfigFactory

object TwitterConfig {

    def setupTwitter()= {
        val conf = ConfigFactory.load()
        val consumerKey = conf.getString("twitter4j.oauth.consumerKey")
        val consumerSecret = conf.getString("twitter4j.oauth.consumerSecret")
        val accessToken = conf.getString("twitter4j.oauth.accessToken")
        val accessTokenSecret = conf.getString("twitter4j.oauth.accessTokenSecret")
        System.setProperty("twitter4j.oauth.consumerKey", consumerKey)
        System.setProperty("twitter4j.oauth.consumerSecret", consumerSecret)
        System.setProperty("twitter4j.oauth.accessToken", accessToken)
        System.setProperty("twitter4j.oauth.accessTokenSecret", accessTokenSecret)
    }
}
