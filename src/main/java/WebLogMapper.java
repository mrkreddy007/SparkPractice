import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;
/**
 * Created by ramakrishna on 1/5/17.
 */
public class WebLogMapper extends Mapper<LongWritable,Text, WebLogReader.WebLogWritable,IntWritable>{

    private static final IntWritable one = new IntWritable((1));

    private WebLogReader.WebLogWritable wlog = new WebLogReader.WebLogWritable();

    private IntWritable reqno = new IntWritable();
    private Text url = new Text();
    private Text rdate= new Text();
    private Text rtime = new Text();
    private Text rip = new Text();

    public  void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String [] words= value.toString().split("\t");
       // System.out.printf("Words[0] - %s, Words[1] - %s,Words[2] - %s, length - %d", words[0],words[1],words[2],words.length);

    reqno.set(Integer.parseInt(words[0]));
    url.set(words[1]);
    rdate.set(words[2]);
    rtime.set(words[3]);
    rip.set(words[4]);

    wlog.set(reqno,url,rdate,rtime,rip);
    context.write(wlog,one);


    }
}
