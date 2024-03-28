package org.rooftop.netx.redis

import org.rooftop.netx.api.Codec
import org.rooftop.netx.api.FailedAckTransactionException
import org.rooftop.netx.api.TransactionManager
import org.rooftop.netx.engine.AbstractTransactionDispatcher
import org.rooftop.netx.engine.core.Transaction
import org.rooftop.netx.meta.TransactionHandler
import org.springframework.context.ApplicationContext
import org.springframework.data.redis.core.ReactiveRedisTemplate
import reactor.core.publisher.Mono

internal class RedisStreamTransactionDispatcher(
    codec: Codec,
    transactionManager: TransactionManager,
    private val applicationContext: ApplicationContext,
    private val reactiveRedisTemplate: ReactiveRedisTemplate<String, Transaction>,
    private val nodeGroup: String,
) : AbstractTransactionDispatcher(codec, transactionManager) {

    override fun findHandlers(): List<Any> {
        return applicationContext.getBeansWithAnnotation(TransactionHandler::class.java)
            .entries.asSequence()
            .map { it.value }
            .toList()
    }

    override fun ack(transaction: Transaction, messageId: String): Mono<Pair<Transaction, String>> {
        return reactiveRedisTemplate.opsForStream<String, ByteArray>()
            .acknowledge(STREAM_KEY, nodeGroup, messageId)
            .map { transaction to messageId }
            .switchIfEmpty(
                Mono.error {
                    throw FailedAckTransactionException("Fail to ack transaction transactionId \"${transaction.id}\" messageId \"$messageId\"")
                }
            )
    }

    private companion object {
        private const val STREAM_KEY = "NETX_STREAM"
    }
}
