import org.apache.spark.{SparkConf, SparkContext}

object task_6 {
  def main(args: Array[String]): Unit = {
    //creating Spark Context
    val conf = new SparkConf().setAppName("task_6").setMaster("local")
    val sc = new SparkContext(conf)
    //reading tweets from source
    val dataFile = sc.textFile("./data/geotweets.tsv")
    //reading stopwords from the source
    val stop_words = sc.textFile("./data/stop_words.txt")
    //broadcasting stopwords
    val stopWords = sc.broadcast(stop_words.collect.toSet)
    //splits data on tab
    val splittedData = dataFile.map(_.split("\t"))
    //filters on US
    val filtered = splittedData.map(row => (row(2), row(10))).filter(row => (row._1 == "US"))
    //get tweet text with lower case
    val tweets = filtered.map(row => row._2.toLowerCase())
    //splits the tweet text into words and filters away words that consists of 1 char
    val words = tweets.flatMap(_.split(" ")).filter(row => row.length >= 2)
    //filters away the words that are in stopwords file
    val word = words.filter(!stopWords.value.contains(_))
    //gives each word a value equal to 1
    val value = word.map(word => (word, 1))
    //aggregates the words by the key thus finding the frequency of each word
    val aggWord = value.aggregateByKey(0)(
      (acc, value) => (acc + value),
      (acc1, acc2) => (acc1 + acc2)
    )
    //swaps the columns such that the frequency of word is in first column and then chooses the top 10 most frequent words
    val top10 = aggWord.map(row => row.swap).top(10)
    //creates an RDD from top10
    val result = sc.parallelize(top10)
    //creates and RDD with result strings
    val output = result.sortBy(row => row._2, ascending = true).sortBy(row => row._1, ascending = false).map(row => row._2 + "\t" + row._1)
    //writes the result to file
    output.coalesce(1).saveAsTextFile("./data/result_6.tvs")

  //stops the spark context
    sc.stop()
  }
}