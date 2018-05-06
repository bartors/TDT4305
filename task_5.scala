import org.apache.spark.{SparkConf, SparkContext}

object task_5 {
  def main(args: Array[String]): Unit = {
    //creating Spark Context
    val conf = new SparkConf().setAppName("task_5").setMaster("local")
    val sc = new SparkContext(conf)
    //reading tweets from source
    val dataFile = sc.textFile("./data/geotweets.tsv")
    //splits data on tab
    val splittedData = dataFile.map(_.split("\t"))
    //filter away those tweets that are not send from a city in US and then maps city name to an RDD and gives each row a value of 1
    val tweets=splittedData.filter(row=>(row(2)=="US"&&row(3)=="city")).map(row=>((row(4),(1))))
    // aggregates the city names by key, thus finding number of tweets from each city
    val aggTweets=tweets.aggregateByKey(0)(
      (acc,value)=>(acc+value),
      (acc1,acc2)=>(acc1+acc2)
    )
    //sorting the cities in descending order according to number of tweets, if the number of tweets is equal for some cities, then they are sorted in alphabetical order.
    val sorted=aggTweets.sortBy(row=>row._1,ascending = true).sortBy(row=>row._2,ascending = false)
    //creates an output string
    val output=sorted.map(row=>(row._1+"\t"+row._2))
    //writes the result to a file
    output.coalesce(1).saveAsTextFile("./data/result_5.tsv")



  //stopping spark context
    sc.stop()
  }
}
