package flink

import flink.schema.CardTransaction
import flink.stateful.CustomerMerchantHabit
import org.apache.flink.api.java.functions.KeySelector
import org.apache.flink.api.java.utils.ParameterTool


object Main extends App {
  import org.apache.flink.streaming.api.scala._

  val params: ParameterTool = ParameterTool.fromArgs(args)
  val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

  // make parameters available in the web interface
  env.getConfig.setGlobalJobParameters(params)

  /**
   * TODO: Switch Data Source to Kafka from Socket.
   */

  val dataStream: DataStream[String] = env
    .socketTextStream("localhost", 9999) // Socket must be up and running


  val incrementalVal = dataStream
    .map(l => {
      val value = l.split("\\W+")
      CardTransaction(value(0).toLong, value(1).toDouble, value(2).toLong)
    })
    .keyBy(
      /**
       * Note:Using a KeySelector function is strictly superior: with Java lambdas
       * they are easy to use and they have potentially less overhead at runtime.
       * {@see https://ci.apache.org/projects/flink/flink-docs-release-1.11/dev/stream/state/state.html#tuple-keys-and-expression-keys}
       * E.g(not efficient): keyBy(l => l.trx_id)
       */
      new KeySelector[CardTransaction, Long] {
        override def getKey(l: CardTransaction): Long = l.trx_id
      }
    )
    .flatMap(new CustomerMerchantHabit())


  incrementalVal.print()

  /**
   * TODO: Write sink function for HBase with JAVA api instead of STDOUT.
   */

  env.execute("Customer-Merchant-Habit")
}