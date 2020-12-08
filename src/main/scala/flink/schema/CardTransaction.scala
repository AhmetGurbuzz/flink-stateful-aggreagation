package flink.schema

case class CardTransaction(
                            trx_id: Long,
                            trx_amount: Double,
                            trx_time: Long
                          )
