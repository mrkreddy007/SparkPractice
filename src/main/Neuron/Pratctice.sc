import org.apache.spark.SparkConf
import org.apache.spark.api.java.JavaPairRDD
import org.apache.spark.api.java.JavaRDD
import org.apache.spark.api.java.JavaSparkContext
import scala.Tuple2
import java.util.Arrays

val JavaConf = new SparkConf
JavaConf.setMaster("local")
JavaConf.setAppName("My Fisrt Java Application")

System.out.println("Creating Spark Context")
val sc = new JavaSparkContext(JavaConf)

JavaRDD<Integer> rdd = sc.parallelize(Arrays.asList(1,2,3,4));
