package org.rooftop.netx.engine

import org.rooftop.netx.api.TransactionEvent
import org.rooftop.netx.api.TransactionManager
import org.rooftop.netx.engine.logging.info
import reactor.core.publisher.Mono
import kotlin.reflect.KClass
import kotlin.reflect.KFunction

internal fun Mono<TransactionEvent>.callOrchestrate(function: OrchestrateDispatchFunction): Mono<*> {
    return this.flatMap {
        function.call(it)
    }
}

internal class OrchestrateDispatchFunction(
    eventType: KClass<*>,
    function: KFunction<Mono<*>>,
    handler: Any,
    noRetryFor: Array<KClass<out Throwable>>,
    nextState: NextTransactionState,
    transactionManager: TransactionManager,
) : AbstractDispatchFunction<Mono<*>>(
    eventType,
    function,
    handler,
    noRetryFor,
    nextState,
    transactionManager,
) {

    override fun call(transactionEvent: TransactionEvent): Mono<*> {
        return Mono.just(transactionEvent)
            .filter { isProcessable(transactionEvent) }
            .map { transactionEvent.copy() }
            .flatMap { function.call(handler, transactionEvent) }
            .info("Call OrchestrateHandler \"${name()}\" with transactionId \"${transactionEvent.transactionId}\"")
            .map {
                publishNextTransaction(transactionEvent)
                it
            }
            .switchIfEmpty(`continue`)
            .onErrorResume {
                if (isNoRollbackFor(it)) {
                    return@onErrorResume noRollbackFor
                }
                `continue`
            }
    }

    private companion object {
        private val `continue` = Mono.just("CONTINUE")
        private val noRollbackFor = Mono.just("NO_ROLLBACK_FOR")
    }
}
