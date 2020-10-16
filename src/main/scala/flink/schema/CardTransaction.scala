package flink.schema

case class CardTransaction(trx_id: Long,
                           amount: Double,
                           trx_time: Long
                          )
