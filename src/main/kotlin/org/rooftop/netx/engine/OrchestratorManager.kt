package org.rooftop.netx.engine

import org.rooftop.netx.api.*
import reactor.core.publisher.Mono
import kotlin.time.Duration.Companion.milliseconds

class OrchestratorManager<T : Any, V : Any> internal constructor(
    private val transactionManager: TransactionManager,
    private val codec: Codec,
    private val orchestratorId: String,
    private val resultHolder: ResultHolder,
    private val orchestrateListener: AbstractOrchestrateListener<T, out Any>,
    private val rollbackOrchestrateListener: AbstractOrchestrateListener<T, out Any>?,
) : Orchestrator<T, V> {

    override fun transactionSync(request: T): Result<V> {
        return transaction(request).block()
            ?: throw TransactionException("Cannot start transaction \"$request\"")
    }

    override fun transactionSync(timeoutMillis: Long, request: T): Result<V> {
        return transaction(timeoutMillis, request).block()
            ?: throw TransactionException("Cannot start transaction \"$request\"")
    }

    override fun transactionSync(request: T, context: MutableMap<String, Any>): Result<V> {
        return transaction(request, context).block()
            ?: throw TransactionException("Cannot start transaction \"$request\"")
    }

    override fun transactionSync(
        timeoutMillis: Long,
        request: T,
        context: MutableMap<String, Any>
    ): Result<V> {
        return transaction(timeoutMillis, request, context).block()
            ?: throw TransactionException("Cannot start transaction \"$request\"")
    }

    override fun transaction(request: T): Mono<Result<V>> {
        return transaction(TEN_SECONDS_TO_TIME_OUT, request, mutableMapOf())
    }

    override fun transaction(timeoutMillis: Long, request: T): Mono<Result<V>> {
        return transaction(timeoutMillis, request, mutableMapOf())
    }

    override fun transaction(request: T, context: MutableMap<String, Any>): Mono<Result<V>> {
        return transaction(TEN_SECONDS_TO_TIME_OUT, request, context)
    }

    override fun transaction(
        timeoutMillis: Long,
        request: T,
        context: MutableMap<String, Any>
    ): Mono<Result<V>> {
        return Mono.just(request)
            .doOnNext { _ ->
                orchestrateListener.setCastableType(request::class)
                rollbackOrchestrateListener?.setCastableType(request::class)
            }
            .map {
                OrchestrateEvent(
                    orchestratorId = orchestratorId,
                    clientEvent = codec.encode(request),
                    context = codec.encode(context.mapValues { codec.encode(it.value) })
                )
            }
            .flatMap { transactionManager.start(it) }
            .flatMap { resultHolder.getResult(timeoutMillis.milliseconds, it) }
    }

    private companion object {
        private const val TEN_SECONDS_TO_TIME_OUT = 10000L
    }
}
