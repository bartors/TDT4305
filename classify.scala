package bartosz.spark

import org.apache.spark.sql.SparkSession
import scala.util.control.Breaks._
object classify {
  def main(args: Array[String]): Unit = {
    //creates SparkContext
    val spark = SparkSession.builder().master("local").appName("Phase_2").getOrCreate()
    //placeholders for the paths
    var testTweets = "placeholder"
    var estimateLocation ="placeholder"
    var outputFile= "placeholder"

    //assigners the path to vars
    for(i <- 0 until args.length by 2){
    if (args(i).equals("-training")){
       testTweets=args(i+1)
    }else if(args(i).equals("-input")){
       estimateLocation=args(i+1)
    }else if (args(i).equals("-output")){
       outputFile = args(i+1)
    }
    }
if (!(testTweets.equals("placeholder")||estimateLocation.equals("placeholders")|| outputFile.equals("placeholder"))) {

  //gets the tweettext
  val text = spark.sparkContext.textFile(estimateLocation).flatMap(_.split(" "))
  //broadcat the tweetWords
  val tweetWords = spark.sparkContext.broadcast(text.collect().toSet)
  //gets the number of words in
  val tweetSize = text.collect().toVector.distinct.size


  //loads the training file
  val trainingFile = spark.sparkContext.textFile(testTweets)
  //splits the training file on tab
  val splittedData = trainingFile.map(_.split("\t"))
  //gets the location name and gives it value 1
  val tweets = splittedData.map(row => (row(4), (1)))
  //count the total number of tweets
  val totNumbOfTweets = tweets.count()
  //count the number of tweet send from each place
  val numofTweets = tweets.aggregateByKey(0)(
    (acc, value) => (acc + value),
    (acc1, acc2) => (acc1 + acc2)
  )
  //calculates the first element of the equation
  val firstPart = numofTweets.map(row => ((row._1), (row._2.toDouble / totNumbOfTweets.toDouble)))
 //spits the tweet text into RDD with one word with respect to the place.
  val keyWords = splittedData.map(row => (row(4), row(10).toLowerCase)).keyBy(row => row._1).flatMapValues(row => row._2.split(" ").distinct)
  //number of words in place
  val placeWords = keyWords.map(row => ((row._1, row._2), 1)).filter(row => tweetWords.value.contains(row._1._2)).aggregateByKey(0)(
    (acc, value) => (acc + value),
    (acc1, acc2) => (acc1 + acc2)
  ).map(row => ((row._1._1), (row._1._2, row._2)))

  //check if a place has a big enough number of valid words, if yes assigners 1, if no 0.
  val numberOfWords = placeWords.map(row => (row._1, 1)).aggregateByKey(0)(
    (acc, value) => (acc + value),
    (acc1, acc2) => (acc1 + acc2)
  ).map(row => if (row._2 == tweetSize) {
    (row._1, 1.toDouble)
  } else {
    (row._1, 0.toDouble)
  })
// joins the number of words from place with the number of tweets from places and divides noumber of words by number of tweets.
  val wordDivTweets = placeWords.join(numofTweets).map(row => ((row._1), (row._2._1._2.toDouble / row._2._2)))
 //multiplies the results from the operation above, then joins the binary value if place has enough words and the first
 // part of equation and multiples everything and then sorts the RDD in descending order.
  val probability = wordDivTweets.aggregateByKey(1.toDouble)(
    (acc, value) => (acc * value),
    (acc1, acc2) => (acc1 * acc2)
  ).join(firstPart).join(numberOfWords).map(row => ((row._1), (row._2._1._1 * row._2._1._2 * row._2._2))).sortBy(_._2, false)
//check if the first row in RD has probability bigger than zero
  if (probability.first()._2 > 0) {
    //assign the probability to a value
    val max = probability.first()._2
    //filter away those places that have probability smaller than the max value
    val words = probability.filter(_._2.equals(max))
    //collect the resting places into a vector
    val vec = words.collect().toVector
    //assign the max value to output string
    var output = max.toString
    //iterate over places
    for (item <- vec) {
      //assign place name to output string
      output = item._1 + "\t" + output
    }
    //  spark.sparkContext.parallelize(output).coalesce(1).saveAsTextFile(outputFile)
    import java.io._
    //wirte the result to file
    val pw = new PrintWriter(new File(outputFile))
    pw.write(output)
    pw.close()
  }
}else{
  //if wrong arguments from commandline
  println("\n\n\nINVALID ARGUMENTS\n\n\nPLEASE USE -training to give the path to trainingFile, -input to give the path to inputTweet and -output to give the path to outputFile\n\n\n")
}
    //stop spark
    spark.sparkContext.stop()

  }

}
