package org.rooftop.netx.engine

import org.rooftop.netx.api.AlreadyCommittedTransactionException
import org.rooftop.netx.api.Codec
import org.rooftop.netx.api.TransactionException
import org.rooftop.netx.api.TransactionManager
import org.rooftop.netx.engine.core.Transaction
import org.rooftop.netx.engine.core.TransactionState
import org.rooftop.netx.engine.logging.info
import org.rooftop.netx.engine.logging.infoOnError
import org.rooftop.netx.engine.logging.warningOnError
import reactor.core.publisher.Mono

internal abstract class AbstractTransactionManager(
    private val codec: Codec,
    private val nodeGroup: String,
    private val nodeName: String,
    private val transactionIdGenerator: TransactionIdGenerator,
) : TransactionManager {

    override fun syncStart(): String {
        return start().block()
            ?: throw TransactionException("Cannot start transaction")
    }

    final override fun <T : Any> syncStart(event: T): String {
        return start(event).block()
            ?: throw TransactionException("Cannot start transaction \"$event\"")
    }

    override fun syncJoin(transactionId: String): String {
        return join(transactionId).block()
            ?: throw TransactionException("Cannot join transaction \"$transactionId\"")
    }

    final override fun <T : Any> syncJoin(transactionId: String, event: T): String {
        return join(transactionId, event).block()
            ?: throw TransactionException("Cannot join transaction \"$transactionId\", \"$event\"")
    }

    final override fun syncExists(transactionId: String): String {
        return exists(transactionId).block()
            ?: throw TransactionException("Cannot exists transaction \"$transactionId\"")
    }

    final override fun syncCommit(transactionId: String): String {
        return commit(transactionId).block()
            ?: throw TransactionException("Cannot commit transaction \"$transactionId\"")
    }

    override fun <T : Any> syncCommit(transactionId: String, event: T): String {
        return commit(transactionId, event).block()
            ?: throw TransactionException("Cannot commit transaction \"$transactionId\" \"$event\"")
    }

    final override fun syncRollback(transactionId: String, cause: String): String {
        return rollback(transactionId, cause).block()
            ?: throw TransactionException("Cannot rollback transaction \"$transactionId\", \"$cause\"")
    }

    override fun <T : Any> syncRollback(transactionId: String, cause: String, event: T): String {
        return rollback(transactionId, cause, event).block()
            ?: throw TransactionException("Cannot rollback transaction \"$transactionId\", \"$cause\" \"$event\"")
    }

    override fun start(): Mono<String> {
        return startTransaction(null)
            .info("Start transaction")
            .contextWrite { it.put(CONTEXT_TX_KEY, transactionIdGenerator.generate()) }
    }

    final override fun <T : Any> start(event: T): Mono<String> {
        return Mono.fromCallable { codec.encode(event) }
            .flatMap { encodedEvent ->
                startTransaction(encodedEvent)
                    .info("Start transaction event \"$event\"")
            }
            .contextWrite { it.put(CONTEXT_TX_KEY, transactionIdGenerator.generate()) }
    }

    private fun startTransaction(event: String?): Mono<String> {
        return Mono.deferContextual<String> { Mono.just(it[CONTEXT_TX_KEY]) }
            .flatMap { transactionId ->
                publishTransaction(
                    transactionId, Transaction(
                        id = transactionId,
                        serverId = nodeName,
                        group = nodeGroup,
                        state = TransactionState.START,
                        event = event,
                    )
                )
            }
    }

    override fun join(transactionId: String): Mono<String> {
        return getAnyTransaction(transactionId)
            .map {
                if (it == TransactionState.COMMIT) {
                    throw AlreadyCommittedTransactionException(transactionId, it.name)
                }
                transactionId
            }
            .warningOnError("Cannot join transaction")
            .flatMap {
                joinTransaction(transactionId, null)
                    .info("Join transaction transactionId \"$transactionId\"")
            }
    }

    override fun <T : Any> join(transactionId: String, event: T): Mono<String> {
        return getAnyTransaction(transactionId)
            .map {
                if (it == TransactionState.COMMIT) {
                    throw AlreadyCommittedTransactionException(transactionId, it.name)
                }
                transactionId
            }
            .warningOnError("Cannot join transaction")
            .map { codec.encode(event) }
            .flatMap {
                joinTransaction(transactionId, it)
                    .info("Join transaction transactionId \"$transactionId\", event \"$event\"")
            }
    }

    private fun joinTransaction(transactionId: String, event: String?): Mono<String> {
        return publishTransaction(
            transactionId, Transaction(
                id = transactionId,
                serverId = nodeName,
                group = nodeGroup,
                state = TransactionState.JOIN,
                event = event,
            )
        )
    }

    final override fun rollback(transactionId: String, cause: String): Mono<String> {
        return exists(transactionId)
            .infoOnError("Cannot rollback transaction cause, transaction \"$transactionId\" is not exists")
            .flatMap {
                rollbackTransaction(transactionId, cause, null)
            }
            .info("Rollback transaction \"$transactionId\"")
            .contextWrite { it.put(CONTEXT_TX_KEY, transactionId) }
    }

    override fun <T : Any> rollback(transactionId: String, cause: String, event: T): Mono<String> {
        return exists(transactionId)
            .infoOnError("Cannot rollback transaction cause, transaction \"$transactionId\" is not exists")
            .map { codec.encode(event) }
            .flatMap { encodedEvent ->
                rollbackTransaction(transactionId, cause, encodedEvent)
            }
            .info("Rollback transaction \"$transactionId\"")
            .contextWrite { it.put(CONTEXT_TX_KEY, transactionId) }
    }

    private fun rollbackTransaction(
        transactionId: String,
        cause: String,
        event: String?
    ): Mono<String> {
        return publishTransaction(
            transactionId, Transaction(
                id = transactionId,
                serverId = nodeName,
                group = nodeGroup,
                state = TransactionState.ROLLBACK,
                cause = cause,
                event = event
            )
        )
    }

    final override fun commit(transactionId: String): Mono<String> {
        return exists(transactionId)
            .infoOnError("Cannot commit transaction cause, transaction \"$transactionId\" is not exists")
            .flatMap { commitTransaction(transactionId, null) }
            .info("Commit transaction \"$transactionId\"")
            .contextWrite { it.put(CONTEXT_TX_KEY, transactionId) }
    }

    override fun <T : Any> commit(transactionId: String, event: T): Mono<String> {
        return exists(transactionId)
            .infoOnError("Cannot commit transaction cause, transaction \"$transactionId\" is not exists")
            .map { codec.encode(event) }
            .flatMap { encodedEvent ->
                commitTransaction(transactionId, encodedEvent)
            }
            .info("Commit transaction \"$transactionId\"")
            .contextWrite { it.put(CONTEXT_TX_KEY, transactionId) }
    }

    private fun commitTransaction(transactionId: String, event: String?): Mono<String> {
        return publishTransaction(
            transactionId, Transaction(
                id = transactionId,
                serverId = nodeName,
                group = nodeGroup,
                state = TransactionState.COMMIT,
                event = event,
            )
        )
    }

    final override fun exists(transactionId: String): Mono<String> {
        return getAnyTransaction(transactionId)
            .infoOnError("There is no transaction corresponding to transactionId \"$transactionId\"")
            .mapTransactionId()
            .contextWrite { it.put(CONTEXT_TX_KEY, transactionId) }
    }

    protected abstract fun getAnyTransaction(transactionId: String): Mono<TransactionState>

    private fun Mono<*>.mapTransactionId(): Mono<String> {
        return this.flatMap {
            Mono.deferContextual { Mono.just(it["transactionId"]) }
        }
    }

    protected abstract fun publishTransaction(
        transactionId: String,
        transaction: Transaction,
    ): Mono<String>

    private companion object {
        private const val CONTEXT_TX_KEY = "transactionId"
    }
}
