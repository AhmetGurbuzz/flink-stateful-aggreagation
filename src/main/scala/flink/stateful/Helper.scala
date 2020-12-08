package flink.stateful

import org.apache.hadoop.hbase.util.Bytes

object Helper {
  def incrementalAvg(incomingValue: Double, currentAvg: Double, N: Long): Double = {
    currentAvg + ((incomingValue - currentAvg) / N)
  }
}