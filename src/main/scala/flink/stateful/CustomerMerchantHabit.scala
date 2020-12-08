package flink.stateful

import Helper._
import flink.schema.{CardTransaction, Habit}
import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration
import org.apache.flink.util.Collector

/**
 * CardTransaction <- IN
 * Habit           <- OUT
 * Calculates customer-merchant relationship habit from CardTransactions
 */
class CustomerMerchantHabit extends RichFlatMapFunction[CardTransaction, Habit] {
  private var habitState: ValueState[Habit] = _

  override def flatMap(input: CardTransaction, out: Collector[Habit]): Unit = {
    val curHbt: Habit = if (habitState.value != null) {
      habitState.value
    } else {
      Habit(input.trx_id, 0, Long.MaxValue, Long.MinValue, input.trx_amount, input.trx_time, input.trx_time)
    }

    var isUpdated = true

    /// Count
    val totalCount: Long = curHbt.count + 1

    /// Min
    val minValue: Double = if (curHbt.min <= input.trx_amount) curHbt.min else input.trx_amount

    /// Max
    val maxValue: Double = if (curHbt.max >= input.trx_amount) curHbt.max else input.trx_amount

    /// Avg
    val avgValue: Double = incrementalAvg(input.trx_amount, curHbt.avg, totalCount)

    /// First Time
    val first: Long = if (curHbt.first_trx <= input.trx_time) curHbt.first_trx else input.trx_time

    /// Last Time
    val last: Long = if (curHbt.last_trx >= input.trx_time) curHbt.last_trx else input.trx_time

    val newHabit: Habit = Habit(curHbt.id, totalCount, minValue, maxValue, avgValue, first, last)

    if (isUpdated) {
      habitState.update(newHabit)
      out.collect(newHabit)
    }

  }

  override def open(parameters: Configuration): Unit = {
    habitState = getRuntimeContext.getState(new ValueStateDescriptor[Habit]("customer_merchant_habit", createTypeInformation[Habit]))
  }
}