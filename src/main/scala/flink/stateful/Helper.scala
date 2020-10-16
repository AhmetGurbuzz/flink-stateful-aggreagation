package flink.stateful

object Helper {
  def incrementalAvg(incomingValue: Double, currentAvg: Double, N: Long): Double = {
    currentAvg + ((incomingValue - currentAvg) / N)
  }
}
