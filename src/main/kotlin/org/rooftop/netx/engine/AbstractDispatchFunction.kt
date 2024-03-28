package org.rooftop.netx.engine

import org.rooftop.netx.api.TransactionEvent
import org.rooftop.netx.api.TransactionManager
import reactor.core.scheduler.Schedulers
import kotlin.reflect.KClass
import kotlin.reflect.KFunction

internal sealed class AbstractDispatchFunction<T>(
    private val eventType: KClass<*>,
    protected val function: KFunction<T>,
    protected val handler: Any,
    private val noRollbackFor: Array<KClass<out Throwable>>,
    private val nextState: NextTransactionState,
    private val transactionManager: TransactionManager,
) {
    fun name(): String = function.name

    abstract fun call(transactionEvent: TransactionEvent): T

    protected fun isNoRollbackFor(throwable: Throwable): Boolean {
        return noRollbackFor.isNotEmpty() && throwable.cause != null && noRollbackFor.contains(
            throwable.cause!!::class
        )
    }

    protected fun isProcessable(transactionEvent: TransactionEvent): Boolean {
        return runCatching {
            transactionEvent.decodeEvent(eventType)
        }.onFailure {
            return it is NullPointerException && eventType == Any::class
        }.isSuccess
    }

    protected fun rollback(transactionEvent: TransactionEvent, throwable: Throwable) {
        transactionEvent.nextEvent?.let {
            transactionManager.rollback(transactionEvent.transactionId, throwable.getCause(), it)
                .subscribeOn(Schedulers.parallel())
                .subscribe()
        } ?: transactionManager.rollback(transactionEvent.transactionId, throwable.getCause())
            .subscribeOn(Schedulers.parallel())
            .subscribe()
    }

    protected fun publishNextTransaction(transactionEvent: TransactionEvent) {
        when (nextState) {
            NextTransactionState.JOIN -> transactionEvent.nextEvent?.let {
                transactionManager.join(transactionEvent.transactionId, it)
            } ?: transactionManager.join(transactionEvent.transactionId)

            NextTransactionState.COMMIT -> transactionEvent.nextEvent?.let {
                transactionManager.commit(transactionEvent.transactionId, it)
            } ?: transactionManager.commit(transactionEvent.transactionId)

            NextTransactionState.END -> return
        }.subscribeOn(Schedulers.parallel())
            .subscribe()
    }

    private fun Throwable.getCause(): String {
        return this.message ?: this.cause?.message ?: this::class.java.name
    }

    internal enum class NextTransactionState {
        JOIN,
        COMMIT,
        END
    }
}
