package org.rooftop.netx.redis

import com.fasterxml.jackson.databind.ObjectMapper
import org.rooftop.netx.api.Codec
import org.rooftop.netx.api.TransactionException
import org.rooftop.netx.engine.AbstractTransactionManager
import org.rooftop.netx.engine.TransactionIdGenerator
import org.rooftop.netx.engine.core.Transaction
import org.rooftop.netx.engine.core.TransactionState
import org.springframework.data.redis.connection.stream.Record
import org.springframework.data.redis.core.ReactiveRedisTemplate
import reactor.core.publisher.Mono

internal class RedisStreamTransactionManager(
    codec: Codec,
    nodeName: String,
    transactionIdGenerator: TransactionIdGenerator,
    private val nodeGroup: String,
    private val reactiveRedisTemplate: ReactiveRedisTemplate<String, Transaction>,
    private val objectMapper: ObjectMapper,
) : AbstractTransactionManager(
    nodeName = nodeName,
    nodeGroup = nodeGroup,
    codec = codec,
    transactionIdGenerator = transactionIdGenerator
) {

    override fun getAnyTransaction(transactionId: String): Mono<TransactionState> {
        return reactiveRedisTemplate
            .opsForHash<String, String>()[transactionId, STATE_KEY]
            .switchIfEmpty(
                Mono.error {
                    throw TransactionException("Cannot find exists transaction id \"$transactionId\"")
                }
            )
            .map { TransactionState.valueOf(it) }
    }

    override fun publishTransaction(transactionId: String, transaction: Transaction): Mono<String> {
        return reactiveRedisTemplate.opsForHash<String, String>()
            .putAll(
                transactionId, mapOf(
                    STATE_KEY to transaction.state.name,
                )
            )
            .map { objectMapper.writeValueAsString(transaction) }
            .flatMap {
                reactiveRedisTemplate.opsForStream<String, Transaction>()
                    .add(
                        Record.of<String, String, String>(mapOf(DATA to it))
                            .withStreamKey(STREAM_KEY)
                    )
            }
            .map { transactionId }
    }

    private companion object {
        private const val DATA = "data"
        private const val STREAM_KEY = "NETX_STREAM"
        private const val STATE_KEY = "TX_STATE"
    }
}
