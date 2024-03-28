package org.rooftop.netx.engine

import org.rooftop.netx.engine.core.Transaction
import org.rooftop.netx.engine.logging.info
import org.rooftop.netx.engine.logging.warningOnError
import reactor.core.publisher.BufferOverflowStrategy
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import reactor.core.scheduler.Schedulers

internal abstract class AbstractTransactionListener(
    private val backpressureSize: Int,
    private val transactionDispatcher: AbstractTransactionDispatcher,
) {

    fun subscribeStream() {
        receive()
            .publishOn(Schedulers.boundedElastic())
            .onBackpressureBuffer(backpressureSize, BufferOverflowStrategy.DROP_LATEST)
            .doOnNext {
                info("Listen transaction ${it.first}\nmessageId \"${it.second}\"")
            }
            .flatMap { (transaction, messageId) ->
                transactionDispatcher.dispatch(transaction, messageId)
                    .warningOnError("Error occurred when listen transaction ${transaction.id}")
            }
            .onErrorResume { Mono.empty() }
            .restartWhenTerminated()
            .subscribe()
    }

    protected abstract fun receive(): Flux<Pair<Transaction, String>>

    private fun <T> Flux<T>.restartWhenTerminated(): Flux<T> {
        return this.doAfterTerminate {
            subscribeStream()
        }
    }
}
