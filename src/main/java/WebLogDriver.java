/**
 * Created by ramakrishna on 1/5/17.
 */
import java.io.IOException;

        import org.apache.hadoop.fs.Path;
        import org.apache.hadoop.io.*;
        import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
        import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
        import org.apache.hadoop.mapreduce.Job;
public class WebLogDriver {


        public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
          args[0]="/Users/ramakrishna/IdeaProjects/SparkPractice/LogInput";
             args[1]="/Users/ramakrishna/IdeaProjects/SparkPractice/LogOutput";
            if(args.length!=2){
                System.err.println("Usage:Comment wordcount <input dir><output dir>\n");
                System.exit(-1);
            }
            Job job = new Job();
            job.setJarByClass(WebLogReader.class);
            job.setJobName("WebLog reader");
            FileInputFormat.addInputPath(job, new Path(args[0]));
            FileOutputFormat.setOutputPath(job, new Path(args[1]));
            job.setMapperClass(WebLogMapper.class);
            job.setReducerClass(WebLogReducer.class);
            job.setMapOutputKeyClass(WebLogReader.WebLogWritable.class);
            job.setMapOutputValueClass(IntWritable.class);
            System.exit(job.waitForCompletion(true)? 0 :1);

    }
}
