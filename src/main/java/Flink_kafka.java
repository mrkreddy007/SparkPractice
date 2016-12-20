/**
 * Created by ramakrishna on 12/13/16.
 */
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer09;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import org.apache.flink.util.Collector;
import org.apache.flink.streaming.api.environment.*;

import java.util.Properties;

public class Flink_kafka {
    public static void main(String[] args) throws Exception {
        // create execution environment
    // create execution environment
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
        properties.setProperty("group.id", "wordcount");

    DataStream<Tuple2<String,Integer>>stream = env.addSource(new FlinkKafkaConsumer09<>(
            "wordcount", new SimpleStringSchema(), properties)).flatMap(new WordCountFlinkDatsStreamApi.Splitter())
            .keyBy(0)
            .timeWindow(Time.milliseconds(50))
            .sum(1);
     stream.print();
    env.execute();
}
    public  static  class Splitter implements FlatMapFunction<String , Tuple2<String,Integer>> {
        @Override
        public  void flatMap(String Sentence,Collector<Tuple2<String,Integer>> out){

            for (String word : Sentence.split(" ")){
                out.collect(new Tuple2<String,Integer>(word,1));
            }
        }
    }
}