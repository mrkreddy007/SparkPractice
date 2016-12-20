/**
 * Created by ramakrishna on 12/6/16.
 */

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.util.Collector;

public class WordCountFlinkDataSetAPI {
public static void main(String[] args) throws Exception {
    final ExecutionEnvironment env =  ExecutionEnvironment.getExecutionEnvironment();

    DataSet<String> locallines = env.readTextFile("/Users/ramakrishna/IdeaProjects/sparktest/input/");


    DataSet<Tuple2<String,Integer>> wordcounts = locallines.flatMap(new LineSplitter())
                                                             .groupBy(0).sum(1);

    wordcounts.print();
    wordcounts.writeAsText("/Users/ramakrishna/IdeaProjects/SparkPractice/Output/Flink/DatasetApi/");

}

    public static class LineSplitter implements FlatMapFunction<String, Tuple2<String, Integer>> {
        @Override
        public void flatMap(String line, Collector<Tuple2<String, Integer>> out) {
            for (String word : line.split(" ")) {
                out.collect(new Tuple2<String, Integer>(word, 1));
            }
        }
    }


}
