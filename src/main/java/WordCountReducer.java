/**
 * Created by ramakrishna on 12/3/16.
 */


import java.io.IOException;

        import org.apache.hadoop.io.*;
        import org.apache.hadoop.mapreduce.Reducer;

public class WordCountReducer extends Reducer<Text,IntWritable,Text,IntWritable> {

    public void reduce(Text key,Iterable<IntWritable> value,Context context) throws IOException, InterruptedException{
        int sum=0;
        for(IntWritable values : value){
            sum+=values.get();

        }
        context.write(key, new IntWritable(sum));
    }
}
