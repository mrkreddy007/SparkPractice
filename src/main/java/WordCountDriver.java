/**
 * Created by ramakrishna on 12/3/16.
 */



import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;

    public class WordCountDriver {

        public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
           // args[0]="Users/ramakrishna/IdeaProjects/sparktest/input/";
           // args[1]="Users/ramakrishna/IdeaProjects/Sparktest/output/";
            if(args.length!=2){
                System.err.println("Usage:Comment wordcount <input dir><output dir>\n");
                System.exit(-1);
            }
            Job job = new Job();
            job.setJarByClass(WordCountDriver.class);
            job.setJobName("WordCount");
            FileInputFormat.addInputPath(job, new Path(args[0]));
            FileOutputFormat.setOutputPath(job, new Path(args[1]));
            job.setMapperClass(WordCountMapper.class);
            job.setReducerClass(WordCountReducer.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(IntWritable.class);
            System.exit(job.waitForCompletion(true)? 0 :1);
        }

    }


