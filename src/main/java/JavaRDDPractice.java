    import org.apache.spark.SparkConf;
    import org.apache.spark.api.java.JavaRDD;
    import org.apache.spark.api.java.JavaSparkContext;
    import org.apache.commons.lang.StringUtils;
    import org.apache.spark.api.java.function.Function;

    import java.util.Arrays;

/**
 * Created by ramakrishna on 12/8/16.
 */
public class JavaRDDPractice {


    public static void main(String[] args) {
        // TODO Auto-generated method stub


        System.out.println("Creating Spark Config");

        SparkConf JavaConf = new SparkConf();
        JavaConf.setMaster("local");
        JavaConf.setAppName("My Fisrt Java Application");

        System.out.println("Creating Spark Context");
        JavaSparkContext jspc = new JavaSparkContext(JavaConf);
        jspc.setLogLevel("WARN");
        JavaRDD<Integer> rdd = jspc.parallelize(Arrays.asList(1,2,3,4));
        JavaRDD<Integer> result =rdd.map(new Function<Integer, Integer>() {
                                             @Override
                                             public Integer call(Integer v1) throws Exception {
                                                 return v1*v1;
                                             }
                                         });
                System.out.println(StringUtils.join(result.collect(), " "));

    }
}
