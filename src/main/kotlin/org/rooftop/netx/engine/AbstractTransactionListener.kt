package org.rooftop.netx.engine

import jakarta.annotation.PreDestroy
import org.rooftop.netx.idl.Transaction
import reactor.core.publisher.BufferOverflowStrategy
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import reactor.core.scheduler.Schedulers

abstract class AbstractTransactionListener(
    private val backpressureSize: Int,
    private val transactionDispatcher: AbstractTransactionDispatcher,
) {

    private var isApplicationShutdown = false

    fun subscribeStream() {
        receive()
            .publishOn(Schedulers.boundedElastic())
            .onBackpressureBuffer(backpressureSize, BufferOverflowStrategy.DROP_LATEST)
            .doOnNext {
                info("Listen transaction \n{\n${it.first}}\nmessageId \"${it.second}\"")
            }
            .flatMap { (transaction, messageId) ->
                transactionDispatcher.dispatch(transaction, messageId)
                    .warningOnError("Error occurred when listen transaction \n{\n$transaction}\nmessageId \"$messageId\"")
                    .onErrorResume { Mono.empty() }
            }
            .onErrorResume { Mono.empty() }
            .restartWhenTerminated()
            .subscribe()
    }

    protected abstract fun receive(): Flux<Pair<Transaction, String>>

    private fun <T> Flux<T>.restartWhenTerminated(): Flux<T> {
        return this.doAfterTerminate {
            if (isApplicationShutdown) {
                return@doAfterTerminate
            }
            subscribeStream()
        }
    }

    @PreDestroy
    private fun shutdownHook() {
        isApplicationShutdown = true
    }
}
