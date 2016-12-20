/**
  * Created by ramakrishna on 12/6/16.
  */
object WordCountFlinkDatasetApiScala {
  import org.apache.flink.api.scala._
  def main(args: Array[String]): Unit ={
 val env = ExecutionEnvironment.getExecutionEnvironment
    val text = env.readTextFile("/Users/ramakrishna/IdeaProjects/sparktest/input/")
    val counts =text.flatMap(line => line.split(" ")).map(word => (word, 1)).groupBy(0).sum(1)
    counts.print()

  }

}
