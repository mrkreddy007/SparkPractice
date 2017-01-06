/**
 * Created by ramakrishna on 1/5/17.
 */
import java.io.IOException;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;

public class WebLogReducer  extends Reducer<WebLogReader.WebLogWritable,IntWritable,Text,IntWritable> {

    private IntWritable result = new IntWritable();
    private Text ip = new Text();

    public void reduce(WebLogReader.WebLogWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum =0;
        ip=key.getIp();

        for (IntWritable val : values){
            sum ++;
        }

    result.set(sum);
   context.write(ip,result);

    }

}
