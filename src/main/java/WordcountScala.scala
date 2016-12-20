/**
  * Created by ramakrishna on 12/2/16.
  */
object WordcountScala {




  import org.apache.spark.{SparkConf,SparkContext}




    def main(args :Array[String]) ={
      println("Creating Spark Configuartion")
      val conf=new SparkConf()
        .setAppName("WordCount")
        .setMaster("local")
      println("Creating Spark Context")
      val sc = new SparkContext(conf)
      println("loading file through spark context")
      val file=sc.textFile("/Users/ramakrishna/Documents/workspace/spark/input/")
      val counts =file.flatMap(line => line.split(" ")).map(word => (word,1)).reduceByKey(_+_)
      counts.foreach(println)
        //saveAsTextFile("/Users/ramakrishna/Documents/workspace/spark/output/")
      sc.stop()

    }

}
