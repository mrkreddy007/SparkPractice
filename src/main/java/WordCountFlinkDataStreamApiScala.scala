/**
  * Created by ramakrishna on 12/6/16.
  */
object WordCountFlinkDataStreamApiScala {
  import org.apache.flink.streaming.api.scala._
  import org.apache.flink.streaming.api.windowing.time.Time

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val text = env.socketTextStream("localhost",9999)

    val counts = text.flatMap{ _.toLowerCase.split("\\W+") filter({_.nonEmpty})}
                        .map {(_,1)}
                          .keyBy(0)
                            .timeWindow(Time.milliseconds(50))
                              .sum(1)
    counts.print()
    env.execute("Window Stream WordCount")
  }

}
