package flink

import flink.schema.{CardTransaction, Habit}
import flink.stateful.CustomerMerchantHabit
import org.apache.flink.api.java.functions.KeySelector
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.environment.CheckpointConfig
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend
import org.apache.flink.connector.hbase.sink.{HBaseMutationConverter, HBaseSinkFunction}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory, Mutation, Put, Table}
import org.apache.hadoop.hbase.util.Bytes

object Main extends App {
  import org.apache.flink.streaming.api.scala._

  val params: ParameterTool = ParameterTool.fromArgs(args)
  val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

  // make parameters available in the web interface
  env.getConfig.setGlobalJobParameters(params)

  env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)
  env.setStateBackend(new RocksDBStateBackend("file:///home/gurbux/Desktop/rocksdb/", false))
  env.enableCheckpointing(10000) // start a checkpoint every 10seconds

  val config = env.getCheckpointConfig
  config.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE) // set mode to exactly-once (this is the default)
  config.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)


  /**
   * TODO: Switch Data Source to Kafka from Socket.
   */

  val dataStream: DataStream[String] = env
    .socketTextStream("localhost", 9999) // Socket must be up and running


  val incrementalVal = dataStream
    .filter(l => l != "")
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

  // create 'customer_habit','0','1','2'
  val conf: Configuration = HBaseConfiguration.create()
  val connection: Connection = ConnectionFactory.createConnection(conf)
  val tableName: TableName = TableName.valueOf("customer_habit")
  val table: Table = connection.getTable(tableName)

  def b(s: String): Array[Byte] = Bytes.toBytes(s)

  val mutationConverter = new HBaseMutationConverter[Habit] {
    override def open(): Unit = {}

    override def convertToMutation(t: Habit): Mutation = {
      new Put(b(t.id.toString))
        .addColumn(
          b((t.id % 4).toString), // Col family
          b(t.id.toString), // Col name
          b(t.productIterator.mkString(",")) // Cell Value
        )
    }
  }

  val hBaseSinkFunction = new HBaseSinkFunction[Habit](
    "customer_habit",
    conf,
    mutationConverter,
    4 * 1024 * 1024,
    50,
    2000
  )

  //incrementalVal.print()
  incrementalVal.addSink(hBaseSinkFunction) /// HBase Sink Function

  env.execute("Customer-Merchant-Habit")
}