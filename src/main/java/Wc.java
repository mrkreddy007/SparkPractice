/**
 * Created by ramakrishna on 12/2/16.
 */


        import org.apache.spark.SparkConf;
        import org.apache.spark.api.java.JavaPairRDD;
        import org.apache.spark.api.java.JavaRDD;
        import org.apache.spark.api.java.JavaSparkContext;

        import scala.Tuple2;
        import java.util.Arrays;

        import org.apache.spark.SparkConf;
        import org.apache.spark.api.java.JavaPairRDD;
        import org.apache.spark.api.java.JavaRDD;
        import org.apache.spark.api.java.JavaSparkContext;

        import scala.Tuple2;
public class Wc {

    public static void main(String[] args)
    {
        // TODO Auto-generated method stub


        System.out.println("Creating Spark Config");

        SparkConf JavaConf = new SparkConf();
        JavaConf.setMaster("local");
        JavaConf.setAppName("My Fisrt Java Application");

        System.out.println("Creating Spark Context");
        JavaSparkContext jspc = new JavaSparkContext(JavaConf);
        System.out.println("loading file through spark context");
        JavaRDD<String> inputRDD =jspc.textFile("/Users/ramakrishna/IdeaProjects/sparktest/input/");
        JavaRDD<String> words =inputRDD.flatMap(line -> Arrays.asList(line.split(" ")).iterator());
        JavaPairRDD<String, Integer> counts =
                words.mapToPair(w -> new Tuple2<String, Integer>(w, 1))
                        .reduceByKey((x, y) -> x + y);
        //counts.saveAsTextFile("/Users/ramakrishna/IdeaProjects/JavaSpark/output/");
        counts.foreach(data -> {System.out.println(data._1()+" "+data._2());});
        jspc.close();
    }



}
