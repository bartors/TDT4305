import org.apache.spark.{SparkConf, SparkContext}

object task_7 {
  def main(args: Array[String]): Unit = {
  //creating Spark Context
  val conf = new SparkConf().setAppName("task_7").setMaster("local")
  val sc = new SparkContext(conf)
  //reading tweets from source
  val dataFile = sc.textFile("./data/geotweets.tsv")
    //loading stopwords
    val stop_words=sc.textFile("./data/stop_words.txt")
    //broadcasting stopwords
    val stopWords=sc.broadcast(stop_words.collect.toSet)
    //splits data on tab
    val splittedData = dataFile.map(_.split("\t"))
    //loads tweets that are from US and a city in RDD
    val tweetsUS=splittedData.filter(row=>(row(2)=="US"&&row(3)=="city"))
    //give each tweet a value of 1
    val tweets=tweetsUS.map(row=>((row(4),(1))))
    //map cities together with tweet text into an RDD
    val tweetsCity=tweetsUS.map(row=>(row(4),row(10)))
    //aggregates number of tweets in each city
    val aggTweets=tweets.aggregateByKey(0)(
      (acc,value)=>(acc+value),
      (acc1,acc2)=>(acc1+acc2)
    )
    //sorts the cities by number of tweets in descending order, and those cites with equal amount of tweets in alphabetical order
    val sorted=aggTweets.sortBy(row=>row._1,ascending = true).sortBy(row=>row._2,ascending = false)
    //chooses 10 cities with most tweets.
    val top5=sc.parallelize(sorted.map(row=>row.swap).top(5))
    // converts the top10 cities to a Seq and then broadcasts it
    val cities=sc.broadcast(top5.map(row=>row._2).collect.toSet)
//filters out those cities that are not in the top10
    val validTweets=tweetsCity.filter(row=>cities.value.contains(row._1))
    // creating an Vector with 5 cites that have most tweets
    val vector= top5.collect.toVector
    //Creating an empty List to store the Strings with results
  var lines=List[String]()
    //Trying a for loop
    for( v <-vector){
      //filter away those tweets that are not from the city you are iterationg over, then split the tweettext into single words.
      val cit=validTweets.filter(row=>row._1==v._2).flatMap(row=>row._2.split(" "))
      //filter away words in "stop_words" file and those words that are shorter than 2 chars. Give each word a value equal to one
      val words=cit.filter(!stopWords.value.contains(_)).filter(row=>row.length>=2).map(row=>(row,1))
      //aggregating by the key (each distinct word); thus effectively finding out how frequent each distinct words is
      val aggWords=words.aggregateByKey(0)(
        (acc,value)=>(acc+value),
        (acc1,acc2)=>(acc1+acc2)
      )
      //swapping the columns then sorting the words descending in frequency, if some words have the same frequency they are sorted in alphabetical order. Taking the 10 most frequent words.
      //you have to swap the columns as the top() method sorts on the key (first column)
      val topWords=aggWords.map(row=>row.swap).sortBy(row=>row._2,ascending = true).sortBy(row=>row._1,ascending = false).top(10)
      //Adding the city and number of tweets to String
      var line=v._2+"\t"+v._1
      //iterating over top 10 words
      for(word<-topWords){
        //adding the word and its frequency to the String
        line=line+"\t"+word._2+"\t"+word._1
      }
      //appending the String to List of Strings
      lines=lines:+line
    }



    //collecting the data and writing to file
    val output=sc.parallelize(lines)
    output.coalesce(1).saveAsTextFile("./data/result_7.tvs")
    //stopss SparcContext
    sc.stop()
}


}
