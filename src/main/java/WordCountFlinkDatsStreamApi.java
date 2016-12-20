/**
 * Created by ramakrishna on 12/6/16.
 */
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

public class WordCountFlinkDatsStreamApi {

    public static void main(String[] args) throws  Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<Tuple2<String,Integer>> dataStream =  env.socketTextStream("localhost", 9999)
                                                             .flatMap(new Splitter())
                                                              .keyBy(0)
                                                               .timeWindow(Time.milliseconds(50))
                                                                .sum(1);
        dataStream.print();
        env.execute("WordCountFlinkDatsStreamApi");

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
