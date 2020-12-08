package flink.schema

case class Habit(
                  id: Long,
                  count: Long,
                  min: Double,
                  max: Double,
                  avg: Double,
                  first_trx: Long,
                  last_trx: Long
                )
