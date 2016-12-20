/**
 * Created by ramakrishna on 12/3/16.
 */


    import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

    public class WordCountMapper extends Mapper<LongWritable,Text,Text,IntWritable> {

        public void map(LongWritable key,Text value, Context context) throws IOException, InterruptedException{
            String Line = value.toString();
            StringTokenizer tokenizer = new StringTokenizer(Line);

            while(tokenizer.hasMoreTokens()){
                String word = tokenizer.nextToken();
                context.write(new Text(word),new IntWritable(1));
            }

        }

    }


