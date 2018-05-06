
import org.apache.spark.sql._


object task_8 {

  def main(args: Array[String]): Unit = {
    //creating Spark Session
    val spark=SparkSession.builder().master("local").appName("task_8").getOrCreate()
    //importing implicits so we can convert RDD to DataFrame
    import spark.implicits._

    //reading tweets from source
    val dataFile = spark.sparkContext.textFile("./data/geotweets.tsv")
    //splits data on tab
    val splittedData = dataFile.map(_.split("\t"))
    //maps RDD to Tweets case class and then converting to DataFrame
    //the columns names found in the case class
    val tweetsDF = splittedData.map(attributes => Tweets(attributes(0).trim.toLong, attributes(1), attributes(2), attributes(3), attributes(4), attributes(5), attributes(6), attributes(7), attributes(8).trim.toLong, attributes(9).trim.toInt, attributes(10), attributes(11).trim.toDouble, attributes(12).trim.toDouble)).toDF()
    //creates a global temporary view from tweetsDF named tweets such that we can run an SQL query on the data.
    tweetsDF.createGlobalTempView("tweets")
    //runs an sql query on the tem.view "twwets" and writes the result to console using show()
    spark.sql("SELECT COUNT(country_name) AS NumberOfTweets, COUNT(DISTINCT username) AS DistinctUsers, COUNT(DISTINCT country_name) as DistinctCountires, COUNT(DISTINCT place_name) as DistinctPlaces, COUNT(DISTINCT language) as DistinctLanguages, MIN(latitude) AS MinLat, MIN(longitude) AS MinLng, MAX(latitude) as MaxLat, MAX(longitude) as MaxLng FROM global_temp.tweets").show()
    //stops the spark environment
    spark.stop()


  }
}
case class Tweets(utc_time: Long, country_name: String, country_code: String, place_type: String, place_name: String, language: String, username: String, user_screen_name: String, timezone_offset: Long, number_of_friends: Int, tweet_text: String, latitude: Double, longitude: Double)


