package org.rooftop.netx.engine

import jakarta.annotation.PreDestroy
import org.rooftop.netx.engine.core.Transaction
import org.rooftop.netx.engine.logging.info
import org.rooftop.netx.engine.logging.warningOnError
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import reactor.core.scheduler.Schedulers
import java.util.concurrent.Executors
import java.util.concurrent.ScheduledExecutorService
import java.util.concurrent.ScheduledFuture
import java.util.concurrent.TimeUnit

internal abstract class AbstractTransactionRetrySupporter(
    private val backpressureSize: Int,
    private val recoveryMilli: Long,
    private val transactionDispatcher: AbstractTransactionDispatcher,
) {

    private lateinit var executor: ScheduledExecutorService;
    private lateinit var scheduledFuture: ScheduledFuture<*>;

    fun watchOrphanTransaction() {
        this.executor = Executors.newSingleThreadScheduledExecutor()

        this.scheduledFuture = executor.scheduleWithFixedDelay(
            handleOrphanTransaction(),
            0,
            recoveryMilli,
            TimeUnit.MILLISECONDS,
        )
    }

    private fun handleOrphanTransaction(): Runnable {
        return Runnable {
            claimOrphanTransaction(backpressureSize)
                .doOnNext {
                    info("Retry orphan transaction ${it.first}\nmessageId \"${it.second}\"")
                }
                .flatMap { (transaction, messageId) ->
                    transactionDispatcher.dispatch(transaction, messageId)
                        .warningOnError("Error occurred when retry orphan transaction \"${transaction.id}\"")
                }
                .onErrorResume { Mono.empty() }
                .subscribeOn(Schedulers.immediate())
                .subscribe()
        }
    }

    protected abstract fun claimOrphanTransaction(backpressureSize: Int): Flux<Pair<Transaction, String>>

    @PreDestroy
    private fun shutdownGracefully() {
        scheduledFuture.cancel(true)
        executor.shutdown()
        runCatching {
            if (!executor.awaitTermination(10, TimeUnit.MINUTES)) {
                executor.shutdownNow()
                if (!executor.awaitTermination(1, TimeUnit.MINUTES)) {
                    org.rooftop.netx.engine.logging.error("Cannot shutdown TransactionRetrySupporter thread")
                }
            }
        }.onFailure {
            executor.shutdownNow()
            Thread.currentThread().interrupt()
        }.onSuccess {
            info("Shutdown TransactionRetrySupporter gracefully")
        }
    }
}
