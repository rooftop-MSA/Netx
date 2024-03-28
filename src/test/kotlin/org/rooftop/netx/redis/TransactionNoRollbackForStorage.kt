package org.rooftop.netx.redis

import org.rooftop.netx.api.TransactionStartEvent
import org.rooftop.netx.api.TransactionStartListener
import org.rooftop.netx.meta.TransactionHandler
import reactor.core.publisher.Mono

@TransactionHandler
class TransactionNoRollbackForStorage {

    @TransactionStartListener(
        RedisDispatcherNoRollbackForTest.IllegalArgumentExceptionEvent::class,
        noRollbackFor = [IllegalArgumentException::class]
    )
    fun noRollbackForIllegalArgumentException(transactionStartEvent: TransactionStartEvent) {
        throw IllegalArgumentException("No Rollback for IllegalArgumentExceptionEvent")
    }

    @TransactionStartListener(
        RedisDispatcherNoRollbackForTest.UnSupportedOperationExceptionEvent::class,
        noRollbackFor = [UnsupportedOperationException::class]
    )
    fun noRollbackForUnSupportedOperationException(transactionStartEvent: TransactionStartEvent): Mono<Unit> {
        throw UnsupportedOperationException("No Rollback for UnSupportedOperationExceptionEvent")
    }

    @TransactionStartListener(
        RedisDispatcherNoRollbackForTest.NoSuchElementExceptionEvent::class,
        noRollbackFor = [UnsupportedOperationException::class]
    )
    fun doRollbackCauseThrowNoSuchElementException(transactionStartEvent: TransactionStartEvent) {
        throw NoSuchElementException("Rollback cause throw NoSuchElementExceptionEvent")
    }
}
