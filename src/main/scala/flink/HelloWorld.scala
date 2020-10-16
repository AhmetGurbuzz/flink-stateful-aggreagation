package flink

import org.apache.flink.api.java.functions.KeySelector
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

object HelloWorld extends App {
  val params: ParameterTool = ParameterTool.fromArgs(args)
  // make parameters available in the web interface
  env.getConfig.setGlobalJobParameters(params)

  // set up the streaming execution environment
  val env = StreamExecutionEnvironment.getExecutionEnvironment

  /*val data: Seq[String] = Seq(
    "Hello Mello Hello",
    "Hello Mello Hello",
    "2nd example of data"
  )*/

  val dataStream = env
    .socketTextStream("localhost", 9999) // Socket must be up and running
  //.fromCollection(data)

  case class WC(word: String, count: Int)

  val counts = dataStream
    .flatMap(_
      .toLowerCase
      .split("\\W+")
      .filter(_.nonEmpty)
    )
    .map(l => WC(l, 1))
    .keyBy(new KeySelector[WC, String] {
      override def getKey(l: WC): String = l.word
    })
    .timeWindow(Time.seconds(10))
    .sum(1)

  counts.print

  env.execute("Window Stream WordCount")
}
