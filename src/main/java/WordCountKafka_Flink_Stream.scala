import java.util.Properties

import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer09
import org.apache.flink.streaming.util.serialization.SimpleStringSchema

/**
  * Created by ramakrishna on 12/13/16.
  */
object WordCountKafka_Flink_Stream {
  import org.apache.flink.streaming.api.scala._
  import org.apache.flink.streaming.api.windowing.time.Time

  def main(args: Array[String]): Unit = {

    val properties = new Properties();
    properties.setProperty("bootstrap.servers","localhost:9092")
    properties.setProperty("group.id", "wordcount");

val env =StreamExecutionEnvironment.getExecutionEnvironment
  val stream =env.addSource(new FlinkKafkaConsumer09[String]("wordcount",new SimpleStringSchema,properties)).flatMap{ _.toLowerCase.split("\\W+") filter({_.nonEmpty})}
    .map {(_,1)}
    .keyBy(0)
    .timeWindow(Time.milliseconds(50))
    .sum(1)
    stream.print()
    env.execute("Window Stream WordCount")




  }





}
