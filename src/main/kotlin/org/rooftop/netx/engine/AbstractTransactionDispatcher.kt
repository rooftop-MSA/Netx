package org.rooftop.netx.engine

import jakarta.annotation.PostConstruct
import org.rooftop.netx.api.*
import org.rooftop.netx.idl.Transaction
import org.rooftop.netx.idl.TransactionState
import org.rooftop.netx.meta.TransactionHandler
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import reactor.core.scheduler.Schedulers
import kotlin.reflect.KClass
import kotlin.reflect.KFunction
import kotlin.reflect.full.declaredMemberFunctions

abstract class AbstractTransactionDispatcher {

    private val monoTransactionHandleFunctions =
        mutableMapOf<TransactionState, MutableList<Pair<KFunction<Mono<*>>, Any>>>()

    private val notPublisherTransactionHandlerFunctions =
        mutableMapOf<TransactionState, MutableList<Pair<KFunction<*>, Any>>>()

    fun dispatch(transaction: Transaction, messageId: String): Flux<Any> {
        return dispatchToMonoHandler(transaction)
            .flatMap { dispatchToNotPublisherHandler(transaction) }
            .doOnComplete {
                ack(transaction, messageId)
                    .subscribeOn(Schedulers.boundedElastic())
                    .subscribe()
            }
    }

    private fun dispatchToMonoHandler(transaction: Transaction): Flux<Any> {
        return Mono.just(transaction.state)
            .filter { state -> monoTransactionHandleFunctions.containsKey(state) }
            .flatMapMany { state ->
                Flux.fromIterable(
                    monoTransactionHandleFunctions[state]
                        ?: throw cannotFindMatchedHandlerFunctionException
                )
            }
            .flatMap { (function, instance) ->
                mapToTransactionEvent(transaction)
                    .flatMap { function.call(instance, it) }
            }
    }

    private fun dispatchToNotPublisherHandler(transaction: Transaction): Flux<*> {
        return Mono.just(transaction.state)
            .filter { state -> notPublisherTransactionHandlerFunctions.containsKey(state) }
            .flatMapMany { state ->
                Flux.fromIterable(
                    notPublisherTransactionHandlerFunctions[state]
                        ?: throw cannotFindMatchedHandlerFunctionException
                )
            }
            .flatMap { (function, instance) ->
                mapToTransactionEvent(transaction)
                    .map { function.call(instance, it) }
            }
    }

    private fun mapToTransactionEvent(transaction: Transaction): Mono<TransactionEvent> {
        return when (transaction.state) {
            TransactionState.TRANSACTION_STATE_START -> Mono.just(
                TransactionStartEvent(
                    transaction.id,
                    transaction.serverId,
                    transaction.group
                )
            )

            TransactionState.TRANSACTION_STATE_COMMIT -> Mono.just(
                TransactionCommitEvent(
                    transaction.id,
                    transaction.serverId,
                    transaction.group,
                )
            )

            TransactionState.TRANSACTION_STATE_JOIN -> Mono.just(
                TransactionJoinEvent(
                    transaction.id,
                    transaction.serverId,
                    transaction.group,
                )
            )

            TransactionState.TRANSACTION_STATE_ROLLBACK -> findOwnUndo(transaction)
                .map {
                    TransactionRollbackEvent(
                        transaction.id,
                        transaction.serverId,
                        transaction.group,
                        transaction.cause,
                        it,
                    )
                }

            else -> throw cannotFindMatchedTransactionEventException
        }
    }

    protected abstract fun findOwnUndo(transaction: Transaction): Mono<String>

    @PostConstruct
    fun initHandler() {
        val transactionHandler = findHandlers(TransactionHandler::class)
        val monoFunctions = getFunctions(transactionHandler, Mono::class)
        monoTransactionHandleFunctions.putAll(monoFunctions)
        val notPublisherFunctions = getNotPublisherFunctions(transactionHandler)
        notPublisherTransactionHandlerFunctions.putAll(notPublisherFunctions)
    }

    @Suppress("unchecked_cast")
    private fun <T : Any> getFunctions(
        foundHandlers: List<Any>,
        returnType: KClass<T>
    ): MutableMap<TransactionState, MutableList<Pair<KFunction<T>, Any>>> {
        val handlers = mutableMapOf<TransactionState, MutableList<Pair<KFunction<T>, Any>>>()

        for (handler in foundHandlers) {
            val returnTypeMatchedHandlers = handler::class.declaredMemberFunctions
                .filter { it.returnType.classifier == returnType }

            returnTypeMatchedHandlers.forEach { function ->
                function.annotations
                    .forEach { annotation ->
                        runCatching {
                            val transactionState = matchedTransactionState(annotation)
                            handlers.putIfAbsent(transactionState, mutableListOf())
                            handlers[transactionState]?.add(function as KFunction<T> to handler)
                        }.onFailure {
                            throw IllegalStateException("Cannot add TransactionHandler", it)
                        }
                    }
            }
        }

        return handlers
    }

    private fun getNotPublisherFunctions(
        foundHandlers: List<Any>
    ): MutableMap<TransactionState, MutableList<Pair<KFunction<*>, Any>>> {
        val handlers = mutableMapOf<TransactionState, MutableList<Pair<KFunction<*>, Any>>>()

        for (handler in foundHandlers) {
            val returnTypeMatchedHandlers = handler::class.declaredMemberFunctions
                .filter { it.returnType.classifier != Mono::class && it.returnType.classifier != Flux::class }

            returnTypeMatchedHandlers.forEach { function ->
                function.annotations
                    .forEach { annotation ->
                        runCatching {
                            val transactionState = matchedTransactionState(annotation)
                            handlers.putIfAbsent(transactionState, mutableListOf())
                            handlers[transactionState]?.add(function to handler)
                        }.onFailure {
                            throw IllegalStateException("Cannot add TransactionHandler", it)
                        }
                    }
            }
        }

        return handlers
    }

    protected abstract fun <T : Annotation> findHandlers(type: KClass<T>): List<Any>

    private fun matchedTransactionState(annotation: Annotation): TransactionState {
        return when (annotation) {
            is TransactionStartHandler -> TransactionState.TRANSACTION_STATE_START
            is TransactionCommitHandler -> TransactionState.TRANSACTION_STATE_COMMIT
            is TransactionJoinHandler -> TransactionState.TRANSACTION_STATE_JOIN
            is TransactionRollbackHandler -> TransactionState.TRANSACTION_STATE_ROLLBACK
            else -> throw notMatchedTransactionHandlerException
        }
    }

    protected abstract fun ack(
        transaction: Transaction,
        messageId: String
    ): Mono<Pair<Transaction, String>>

    private companion object {
        private val cannotFindMatchedTransactionEventException =
            java.lang.IllegalStateException("Cannot find matched transaction event")

        private val cannotFindMatchedHandlerFunctionException =
            IllegalStateException("Cannot find matched handler function")

        private val notMatchedTransactionHandlerException =
            IllegalStateException("Cannot find matched Transaction handler")
    }
}
