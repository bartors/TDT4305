
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf





object task_2 {
  def main(args: Array[String]): Unit = {
    //creating Spark Context
    val conf = new SparkConf().setAppName("task_2").setMaster("local")
    val sc = new SparkContext(conf)
    //reading tweets from source
    val dataFile = sc.textFile("./data/geotweets.tsv")
    //splits data on tab
    val splittedData =dataFile.map(_.split("\t"))
    // gets the countryNames and value 1
    val countryName=splittedData.map(row=>(row(1),1))
    //aggregates by the country, and sorts in descending order by number of tweets, if some countries have equal amount of tweets then they are sorted in alphabetical order
    val aggregatedValues = countryName.aggregateByKey(0)(
      (acc,value)=>(acc+value),
      (acc1,acc2)=>(acc1+acc2)
    ).sortBy(row=>row._1,ascending = true).sortBy(row=>row._2,ascending = false)
    //formats for country"\t"value
    val write=aggregatedValues.map(row=>(row._1+"\t"+row._2))
    //writes  to file using coalesce
    write.coalesce(1).saveAsTextFile("./data/result_2.tsv")


    // stoppping the Spark Context
    sc.stop()
  }

}
