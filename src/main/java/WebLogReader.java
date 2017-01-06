import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import java.io.DataInput;
import  org.apache.hadoop.io.*;
import java.io.*;


/**
 * Created by ramakrishna on 1/5/17.
 */
public class WebLogReader {

    public static class WebLogWritable implements WritableComparable<WebLogWritable> {
        private Text siteURl, reqDate,timestamp,ipaddress;
        private IntWritable reqNo;


        // Default Constructor

        public WebLogWritable(){
            this.siteURl= new Text();
            this.ipaddress=new Text();
            this.timestamp= new Text();
            this.reqDate = new Text();
            this.reqNo= new IntWritable();
        }
        //custom constructor
        public WebLogWritable(IntWritable reqno,Text url, Text rdate, Text rtime ,Text rip){
            this.siteURl=url;
            this.reqNo=reqno;
            this.reqDate=rdate;
            this.timestamp=rtime;
            this.ipaddress=rip;
        }
        // Setter Method to set values of Hadoopcustom Writable objects
        public void set(IntWritable reqno,Text url, Text rdate , Text rtime, Text rip){
            this.siteURl= url;
            this.ipaddress=rip;
            this.reqDate=rdate;
            this.reqNo= reqno;
            this.timestamp=rtime;
        }


        public Text getIp()
        {
            return ipaddress;
        }

        @Override
        //over riding default readFields Method
        //It de-serializes the byte stream data

        public void readFields(DataInput in ) throws IOException{
            ipaddress.readFields(in);
            timestamp.readFields(in);
            reqDate.readFields(in);
            reqNo.readFields(in);
            siteURl.readFields(in);
        }
        @Override
        // It serializes the object data into byte stream data
        public void write(DataOutput out) throws IOException{
            ipaddress.write(out);
            timestamp.write(out);
            reqDate.write(out);
            reqNo.write(out);
            siteURl.write(out);
        }

        public int compareTo(WebLogWritable o)
        {
            if (ipaddress.compareTo(o.ipaddress)==0)
                return (timestamp.compareTo(o.timestamp));
            else return (ipaddress.compareTo(o.ipaddress));
        }

        @Override
        public boolean equals(Object o){
            if ( o instanceof WebLogWritable)
            {
                WebLogWritable other = (WebLogWritable) o;
                return ipaddress.equals(other.ipaddress) && timestamp.equals(other.timestamp);
            }
            else return  false;
        }
        @Override
        public  int hashCode(){

            return ipaddress.hashCode();
        }

    }


}
