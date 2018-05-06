import org.apache.spark.{SparkConf, SparkContext}
import java.time.ZoneId
import java.time.Instant
import java.time.LocalTime

object task_4 {
  def main(args: Array[String]): Unit = {
    //creating Spark Context
    val conf = new SparkConf().setAppName("task_4").setMaster("local")
    val sc = new SparkContext(conf)
    //reading tweets from source
    val dataFile = sc.textFile("./data/geotweets.tsv")
    //splits data on tab
    val splittedData = dataFile.map(_.split("\t"))

    //the commented lines below describes what is happening at line 26
/* //makes java.UTC
    val UTC = splittedData.map(row=>(row(0).toLong+row(8).toLong*1000))
    //makes java.Instant
    val instant=UTC.map(row=>Instant.ofEpochMilli(row))
    //makes java.LocalTime
    val localTime=instant.map(row=>LocalTime.from(row.atZone(ZoneId.of("UTC"))))
    //get the hour of the tweet
    val hour=localTime.map(row=>row.getHour)*/
    //makes rdd as described above and give each row a value of 1. the key of each row is country_name and the hour of the tweet
    val tweets=splittedData.map(row=>(((row(1),((LocalTime.from((Instant.ofEpochMilli(row(0).toLong+row(8).toLong*1000)).atZone(ZoneId.of("UTC")))).getHour)),((1)))))
  //aggregates the hours thus finding the number of tweets of each hour in each country
    val aggregTweets=tweets.aggregateByKey(0)(
      (acc,value)=>(acc+value),
        (acc1,acc2)=>(acc1+acc2)
    )
    //creates result string
    val output=aggregTweets.map(row=>(row._1._1+"\t"+row._1._2+"\t"+row._2))
    //writes the result to a file
    output.coalesce(1).saveAsTextFile("./data/task_4.tsv")
    //stops the spark context
    sc.stop()
  }

}
