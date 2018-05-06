import org.apache.spark.SparkContext
import org.apache.spark.SparkConf



object task_1 {
  def main(args: Array[String]): Unit = {
    //creating Spark Context
    val conf = new SparkConf().setAppName("task_1").setMaster("local")
    val sc = new SparkContext(conf)
    //reading tweets from source
    val dataFile = sc.textFile("./data/geotweets.tsv")
    //splits data on tab
    val splittedData =dataFile.map(_.split("\t"))
    // gets the username column
    val users=splittedData.map(row=>row(6))
    // gets the countryNames
    val countryName=splittedData.map(row=>row(1))
    //gets the placeNames
    val places=splittedData.map(row=>row(4))
    //gets the languages
    val language=splittedData.map(row=>row(5))
    //gets the latitude
    val lat=splittedData.map(row=>row(11).toFloat)
    //gets the longitude
    val lon=splittedData.map(row =>row(12).toFloat)
    //gets the tweet
    val tweets=splittedData.map(row=>row(10))
    //gets the tweets length in char
    val tweetChar=tweets.map(tweet=>tweet.length())
    //agregate the length of tweetChar
    val agrTweetChar=tweetChar.aggregate(0)(
      (acc,value)=>(acc+value),
      (acc1, acc2) =>(acc1+acc2)
    )
    // split the tweets in words
    val tweetsSplitted=tweets.map(_.split(" "))
    // get the number of words
    val tweetsWord=tweetsSplitted.map(tweet=>tweet.size)
    //agregate the number of words
    val argTweetWord=tweetsWord.aggregate(0)(
      (acc, value)=>(acc + value),
        (acc1, acc2)=>(acc1+acc2)
      )
    //a gets the number of tweets
    val tweetNumb=users.count()
    //b get the number of distinct users
    val distinctUsers=users.distinct().count()
    //c gets the number of distinct countries
    val distinctCountries=countryName.distinct().count()
    //d gets the number of distinct places
    val distinctPlaces=places.distinct().count()
    //e gets the number of distinct languages
    val distinctLanguages=language.distinct().count()
    //f gets the minimal latitude
    val minLat=lat.min()
    //g gets the minimal longitude
    val minLon=lon.min()
    //h gets the maximal latitude
    val maxLat=lat.max()
    //i gets the maximal longitude
    val maxLon=lon.max()
    //j gets the average number of chars in a tweets
    val averageTweetChar= agrTweetChar/tweetNumb
    //k gets the average number of words in tweet
    val averageTweetWords=argTweetWord/tweetNumb


  //creates the result string
  val userNumb=sc.parallelize(Seq(tweetNumb, distinctUsers,distinctCountries,distinctPlaces,distinctLanguages,minLat,minLon,maxLat,maxLon, averageTweetChar, averageTweetWords ))
    // saves the result to file, no need for coalesce as the RDD consists one one row, and thus have one partition.
    userNumb.coalesce(1).saveAsTextFile("./data/result_1.tsv")



    // stoppping the Spark Context
    sc.stop()
  }

}
