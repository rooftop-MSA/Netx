package org.rooftop.netx.engine

import org.rooftop.netx.api.TransactionEvent
import org.rooftop.netx.api.TransactionManager
import org.rooftop.netx.engine.logging.info
import reactor.core.publisher.Mono
import kotlin.reflect.KClass
import kotlin.reflect.KFunction

internal fun Mono<TransactionEvent>.callNotPublish(function: NotPublishDispatchFunction): Mono<*> {
    return this.map { function.call(it) }
}

internal class NotPublishDispatchFunction(
    eventType: KClass<*>,
    function: KFunction<*>,
    handler: Any,
    noRollbackFor: Array<KClass<out Throwable>>,
    nextState: NextTransactionState,
    transactionManager: TransactionManager,
) : AbstractDispatchFunction<Any?>(
    eventType,
    function,
    handler,
    noRollbackFor,
    nextState,
    transactionManager,
) {

    override fun call(transactionEvent: TransactionEvent): Any {
        if (isProcessable(transactionEvent)) {
            return runCatching {
                function.call(handler, transactionEvent)
                info("Call NotPublisher TransactionHandler \"${name()}\" with transactionId \"${transactionEvent.transactionId}\"")
            }.fold(
                onSuccess = { publishNextTransaction(transactionEvent) },
                onFailure = {
                    if (isNoRollbackFor(it)) {
                        return@fold NO_ROLLBACK_FOR
                    }
                    rollback(transactionEvent, it)
                },
            )
        }
        return SKIP
    }

    private companion object {
        private const val NO_ROLLBACK_FOR = "SUCCESS"
        private const val SKIP = "SKIP"
    }
}
