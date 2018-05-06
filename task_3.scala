import org.apache.spark.{SparkConf, SparkContext}

object task_3 {
  def main(args: Array[String]): Unit = {
    //creating Spark Context
    val conf = new SparkConf().setAppName("task_3").setMaster("local")
    val sc = new SparkContext(conf)
    //reading tweets from source
    val dataFile = sc.textFile("./data/geotweets.tsv")
    //splits data on tab
    val splittedData =dataFile.map(_.split("\t"))
    // creates and RDD with country name as key, and latitude as the first value, longitude as the second value and 1 as the third
    val tuple=splittedData.map(row=>((row(1)),(row(11).toFloat,row(12).toFloat,1)))
    //aggregates by the key, in effect get the sum of latitude, longitude and number of tweets for each county
    val aggTuple=tuple.reduceByKey((x,y)=>(x._1+y._1,x._2+y._2,x._3+y._3))

    //filer away countries with tweets<=10
    val filltered=aggTuple.filter(row=>row._2._3>10)
  //creates the result string by writing country_name. mean latitude and mean longitude
    val write=filltered.map(row=>(row._1+"\t"+row._2._1/row._2._3+"\t"+row._2._2/row._2._3))
  //writes the result to file
    write.coalesce(1).saveAsTextFile("./data/result_3.tsv")
    // stopping the Spark Context
    sc.stop()

}}
